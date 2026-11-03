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

//! Keeps the gRPC API server's mTLS identity in sync with cert-manager
//! renewals, without a process restart.
//!
//! cert-manager rewrites `tls.cert` / `tls.key` on disk in place when it
//! renews the certificate; nothing else in the deployment observes that
//! change. [`spawn_cert_reload_task`] polls those files on the configured
//! interval (default [`CERT_RELOAD_INTERVAL`]) and swaps the identity handed
//! out on new mTLS handshakes via [`ReloadableCertResolver`]. Renewal happens
//! on the order of weeks, so a bounded poll delay is preferable to a
//! filesystem watcher: it is simpler and has no dependency on how
//! cert-manager performs its atomic file swap (symlink rename vs in-place
//! write).
//!
//! The client CA root is not covered here -- only the server's own leaf
//! identity is expected to rotate.
//!
//! tonic's own `ServerTlsConfig` reads the identity once when
//! [`tonic::transport::Server::tls_config`] is called and has no reload
//! hook, so the mTLS handshake is terminated here instead, ahead of handing
//! the plain-old already-TLS-wrapped connection to tonic's router via
//! `serve_with_incoming_shutdown`.

use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::result::Result;
use std::sync::{Arc, PoisonError, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use rustls::pki_types::CertificateDer;
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{RootCertStore, ServerConfig};
use rustls_pemfile::{certs, private_key};
use tokio::net::TcpStream;
use tokio::task::JoinSet;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::server::TcpIncoming;

/// The h2 ALPN identifier tonic's own mTLS listener negotiates. Matched here
/// so switching to a custom acceptor changes no client-visible behavior.
const ALPN_H2: &[u8] = b"h2";

/// Default poll period when `[tls] cert_reload_interval_seconds` is omitted.
/// Renewal cadence is on the order of 30 days, so a multi-minute detection
/// delay is an acceptable trade for the simplicity of polling over a
/// filesystem watcher.
pub const CERT_RELOAD_INTERVAL: Duration =
    Duration::from_secs(crate::config::DEFAULT_CERT_RELOAD_INTERVAL_SECONDS);

/// Serves the identity presented on new mTLS handshakes; swapped in place by
/// [`spawn_cert_reload_task`] when it detects a renewed cert/key pair.
/// Connections already established keep the identity presented at their own
/// handshake time -- only new connections observe a rotation.
pub struct ReloadableCertResolver {
    current: RwLock<Arc<CertifiedKey>>,
}

impl std::fmt::Debug for ReloadableCertResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReloadableCertResolver")
            .finish_non_exhaustive()
    }
}

impl ReloadableCertResolver {
    /// Returns the installed identity.
    ///
    /// A poisoned lock is recovered. The value is an `Arc`, so a writer panic
    /// cannot tear it, and treating poison as fatal would fail new handshakes
    /// while the process stays up.
    fn current_identity(&self) -> Arc<CertifiedKey> {
        self.current
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl ResolvesServerCert for ReloadableCertResolver {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.current_identity())
    }
}

/// Swaps in `certified_key` when it differs from the installed identity.
/// Recovers a poisoned lock for the same reason as
/// [`ReloadableCertResolver::current_identity`].
fn install_certified_key(resolver: &ReloadableCertResolver, certified_key: CertifiedKey) -> bool {
    let mut current = resolver
        .current
        .write()
        .unwrap_or_else(PoisonError::into_inner);
    if same_identity(current.as_ref(), &certified_key) {
        false
    } else {
        *current = Arc::new(certified_key);
        true
    }
}

fn parse_certs(pem: &[u8]) -> std::result::Result<Vec<CertificateDer<'static>>, String> {
    let mut cursor: &[u8] = pem;
    certs(&mut cursor)
        .collect::<Result<_, _>>()
        .map_err(|e| format!("failed to parse certificate PEM: {e}"))
}

fn load_certified_key(
    cert_pem: &[u8],
    key_pem: &[u8],
) -> std::result::Result<CertifiedKey, String> {
    let certs = parse_certs(cert_pem)?;
    if certs.is_empty() {
        return Err("no certificates found in server certificate PEM".into());
    }

    let key = private_key(&mut &*key_pem)
        .map_err(|e| format!("failed to parse server key PEM: {e}"))?
        .ok_or_else(|| "no private key found in server key PEM".to_string())?;

    let provider = rustls::crypto::aws_lc_rs::default_provider();
    // Reject a pair that parses but does not belong together. cert-manager can
    // publish the renewed certificate before the matching key, and installing
    // that pair would replace a working identity.
    CertifiedKey::from_der(certs, key, &provider)
        .map_err(|e| format!("server certificate does not match its private key: {e}"))
}

fn same_identity(current: &CertifiedKey, renewed: &CertifiedKey) -> bool {
    current.cert == renewed.cert && current.key.public_key() == renewed.key.public_key()
}

/// Builds the mTLS `rustls::ServerConfig` used for the gRPC listener: client
/// certificates are required and verified against `ca_pem`, and the server
/// identity is served through the returned [`ReloadableCertResolver`] so it
/// can be swapped without rebuilding the config.
pub fn build_reloadable_server_config(
    cert_pem: &[u8],
    key_pem: &[u8],
    ca_pem: &[u8],
) -> std::result::Result<(Arc<ServerConfig>, Arc<ReloadableCertResolver>), String> {
    let certified_key = load_certified_key(cert_pem, key_pem)?;
    let resolver = Arc::new(ReloadableCertResolver {
        current: RwLock::new(Arc::new(certified_key)),
    });

    let mut roots = RootCertStore::empty();
    let (added, ignored) = roots.add_parsable_certificates(parse_certs(ca_pem)?);
    if added == 0 || ignored > 0 {
        return Err(format!(
            "failed to parse client CA certificate PEM ({added} accepted, {ignored} rejected)"
        ));
    }

    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|e| format!("failed to build client certificate verifier: {e}"))?;

    // Documented as TLS 1.3 only. ServerConfig::builder() also enables TLS 1.2.
    let mut config = ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_client_cert_verifier(verifier)
        .with_cert_resolver(resolver.clone());
    config.alpn_protocols.push(ALPN_H2.to_vec());

    Ok((Arc::new(config), resolver))
}

/// Spawns the background task that re-reads `cert_path`/`key_path` every
/// `interval` and swaps `resolver`'s identity when the files contain a new
/// matching pair. A read, parse, or key-mismatch failure is logged and the
/// previous identity is kept -- a transient error (e.g. cert-manager
/// mid-write) must not tear down a server that is otherwise healthy. An
/// unchanged pair is left in place. `interval` must be non-zero; the default
/// is [`CERT_RELOAD_INTERVAL`].
pub fn spawn_cert_reload_task(
    cert_path: PathBuf,
    key_path: PathBuf,
    resolver: Arc<ReloadableCertResolver>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(interval);
        // The identity loaded at startup is already current; the first tick
        // fires immediately, so skip it and wait a full interval before the
        // first reload attempt.
        interval.tick().await;

        loop {
            interval.tick().await;

            match reload_once(&cert_path, &key_path).await {
                Ok(certified_key) => {
                    let changed = install_certified_key(&resolver, certified_key);
                    if changed {
                        tracing::info!(
                            cert_path = %cert_path.display(),
                            key_path = %key_path.display(),
                            "reloaded gRPC server certificate"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        cert_path = %cert_path.display(),
                        key_path = %key_path.display(),
                        error = %e,
                        "failed to reload gRPC server certificate; keeping current identity"
                    );
                }
            }
        }
    })
}

async fn reload_once(
    cert_path: &Path,
    key_path: &Path,
) -> std::result::Result<CertifiedKey, String> {
    let cert_pem = tokio::fs::read(cert_path)
        .await
        .map_err(|e| format!("failed to read {}: {e}", cert_path.display()))?;
    let key_pem = tokio::fs::read(key_path)
        .await
        .map_err(|e| format!("failed to read {}: {e}", key_path.display()))?;

    load_certified_key(&cert_pem, &key_pem)
}

/// Wraps [`TcpIncoming`] so each accepted TCP connection is terminated as
/// mTLS before being handed to tonic's router. Handshakes run concurrently
/// (via a [`JoinSet`]) so a slow or stalled client handshake cannot delay
/// accepting the next connection -- mirroring the concurrency tonic's own
/// built-in TLS support uses internally.
pub struct TlsIncoming {
    incoming: TcpIncoming,
    acceptor: TlsAcceptor,
    shutdown: CancellationToken,
    handshakes: JoinSet<io::Result<TlsStream<TcpStream>>>,
}

impl TlsIncoming {
    pub fn new(incoming: TcpIncoming, acceptor: TlsAcceptor, shutdown: CancellationToken) -> Self {
        Self {
            incoming,
            acceptor,
            shutdown,
            handshakes: JoinSet::new(),
        }
    }
}

impl Stream for TlsIncoming {
    type Item = io::Result<TlsStream<TcpStream>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Drain any handshake that has already finished before accepting
        // more connections, so completed connections are not held up behind
        // `poll_accept`.
        if let Poll::Ready(Some(result)) = self.handshakes.poll_join_next(cx) {
            return Poll::Ready(Some(match result {
                Ok(accepted) => accepted,
                Err(join_err) => Err(io::Error::other(join_err)),
            }));
        }

        match Pin::new(&mut self.incoming).poll_next(cx) {
            Poll::Ready(Some(Ok(tcp_stream))) => {
                let acceptor = self.acceptor.clone();
                let shutdown = self.shutdown.clone();
                self.handshakes.spawn(async move {
                    tokio::select! {
                        result = acceptor.accept(tcp_stream) => result,
                        _ = shutdown.cancelled() => Err(io::Error::new(
                            io::ErrorKind::ConnectionAborted,
                            "gRPC shutdown",
                        )),
                    }
                });
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;

    fn generate_pem_pair(name: &str) -> (Vec<u8>, Vec<u8>) {
        let generated = rcgen::generate_simple_self_signed(vec![name.to_owned()]).unwrap();
        (
            generated.cert.pem().into_bytes(),
            generated.key_pair.serialize_pem().into_bytes(),
        )
    }

    fn install_crypto_provider() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }

    #[test]
    fn build_reloadable_server_config_requires_parsable_certs() {
        install_crypto_provider();
        let (cert_pem, key_pem) = generate_pem_pair("localhost");
        let err = build_reloadable_server_config(&cert_pem, &key_pem, b"not a pem")
            .expect_err("garbage CA PEM must be rejected");
        assert!(err.contains("client CA"), "unexpected error: {err}");
    }

    #[test]
    fn build_reloadable_server_config_builds_with_matching_material() {
        install_crypto_provider();
        let (cert_pem, key_pem) = generate_pem_pair("localhost");
        let (ca_pem, _) = generate_pem_pair("ca.example.com");

        build_reloadable_server_config(&cert_pem, &key_pem, &ca_pem)
            .expect("valid cert/key/ca material should build a server config");
    }

    fn cert_der(resolver: &ReloadableCertResolver) -> Vec<u8> {
        resolver.current_identity().cert[0].as_ref().to_vec()
    }

    #[test]
    fn poisoned_cert_lock_still_serves_and_accepts_a_new_identity() {
        install_crypto_provider();
        let (initial_pem, initial_key) = generate_pem_pair("poison.example.com");
        let initial = load_certified_key(&initial_pem, &initial_key).unwrap();
        let initial_der = initial.cert[0].as_ref().to_vec();
        let resolver = Arc::new(ReloadableCertResolver {
            current: RwLock::new(Arc::new(initial)),
        });

        let poisoner = Arc::clone(&resolver);
        let join = std::thread::spawn(move || {
            let _guard = poisoner.current.write().unwrap();
            panic!("poison the cert resolver lock");
        });
        assert!(join.join().is_err(), "the poisoner thread should panic");
        assert_eq!(cert_der(&resolver), initial_der);

        let (renewed_pem, renewed_key) = generate_pem_pair("after-poison.example.com");
        let renewed = load_certified_key(&renewed_pem, &renewed_key).unwrap();
        let renewed_der = renewed.cert[0].as_ref().to_vec();
        assert!(install_certified_key(&resolver, renewed));
        assert_eq!(cert_der(&resolver), renewed_der);
    }

    #[test]
    fn load_certified_key_rejects_a_certificate_that_does_not_match_the_key() {
        install_crypto_provider();
        let (cert_pem, _key_pem) = generate_pem_pair("server.example.com");
        let (_other_cert, other_key) = generate_pem_pair("other.example.com");

        let err = load_certified_key(&cert_pem, &other_key)
            .expect_err("a parsed certificate and an unrelated key must be rejected");
        assert!(err.contains("does not match"), "unexpected error: {err}");
    }

    #[tokio::test(start_paused = true)]
    async fn reload_task_keeps_the_current_identity_until_a_matching_pair_is_available() {
        install_crypto_provider();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");

        let (initial_cert, initial_key) = generate_pem_pair("initial.example.com");
        tokio::fs::write(&cert_path, &initial_cert).await.unwrap();
        tokio::fs::write(&key_path, &initial_key).await.unwrap();
        let initial = load_certified_key(&initial_cert, &initial_key).unwrap();
        let initial_der = initial.cert[0].as_ref().to_vec();
        let resolver = Arc::new(ReloadableCertResolver {
            current: RwLock::new(Arc::new(initial)),
        });
        let task = spawn_cert_reload_task(
            cert_path.clone(),
            key_path.clone(),
            resolver.clone(),
            CERT_RELOAD_INTERVAL,
        );

        // The startup tick is skipped, so a renewal written immediately is
        // not observed yet.
        let (early_cert, early_key) = generate_pem_pair("early.example.com");
        tokio::fs::write(&cert_path, &early_cert).await.unwrap();
        tokio::fs::write(&key_path, &early_key).await.unwrap();
        drive_reload_task().await;
        assert_eq!(cert_der(&resolver), initial_der);

        tokio::fs::write(&cert_path, b"not a certificate")
            .await
            .unwrap();
        tokio::time::advance(CERT_RELOAD_INTERVAL).await;
        drive_reload_task().await;
        assert_eq!(cert_der(&resolver), initial_der);

        let (mismatched_cert, _mismatched_key) = generate_pem_pair("mismatched.example.com");
        tokio::fs::write(&cert_path, &mismatched_cert)
            .await
            .unwrap();
        tokio::fs::write(&key_path, &initial_key).await.unwrap();
        tokio::time::advance(CERT_RELOAD_INTERVAL).await;
        drive_reload_task().await;
        assert_eq!(cert_der(&resolver), initial_der);

        let (renewed_cert, renewed_key) = generate_pem_pair("renewed.example.com");
        tokio::fs::write(&cert_path, &renewed_cert).await.unwrap();
        tokio::fs::write(&key_path, &renewed_key).await.unwrap();
        tokio::time::advance(CERT_RELOAD_INTERVAL).await;
        drive_reload_task().await;
        let expected = load_certified_key(&renewed_cert, &renewed_key).unwrap();
        assert_eq!(cert_der(&resolver), expected.cert[0].as_ref());

        let installed = resolver.current_identity();
        tokio::time::advance(CERT_RELOAD_INTERVAL).await;
        drive_reload_task().await;
        assert!(Arc::ptr_eq(&installed, &resolver.current_identity()));

        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn reload_task_honors_a_shorter_interval() {
        install_crypto_provider();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        let (initial_cert, initial_key) = generate_pem_pair("initial.example.com");
        tokio::fs::write(&cert_path, &initial_cert).await.unwrap();
        tokio::fs::write(&key_path, &initial_key).await.unwrap();
        let initial = load_certified_key(&initial_cert, &initial_key).unwrap();
        let initial_der = initial.cert[0].as_ref().to_vec();
        let resolver = Arc::new(ReloadableCertResolver {
            current: RwLock::new(Arc::new(initial)),
        });
        let interval = Duration::from_secs(30);
        let task = spawn_cert_reload_task(
            cert_path.clone(),
            key_path.clone(),
            resolver.clone(),
            interval,
        );

        let (renewed_cert, renewed_key) = generate_pem_pair("sooner.example.com");
        tokio::fs::write(&cert_path, &renewed_cert).await.unwrap();
        tokio::fs::write(&key_path, &renewed_key).await.unwrap();
        // Consume the skipped startup tick before moving the clock, so the
        // period is measured from t = 0.
        drive_reload_task().await;
        assert_eq!(cert_der(&resolver), initial_der);

        tokio::time::advance(interval - Duration::from_secs(1)).await;
        drive_reload_task().await;
        assert_eq!(cert_der(&resolver), initial_der);

        tokio::time::advance(Duration::from_secs(1)).await;
        drive_reload_task().await;
        let expected = load_certified_key(&renewed_cert, &renewed_key).unwrap();
        assert_eq!(cert_der(&resolver), expected.cert[0].as_ref());

        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn reload_task_keeps_the_current_identity_when_the_certificate_file_is_removed() {
        install_crypto_provider();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");

        let (initial_cert, initial_key) = generate_pem_pair("initial.example.com");
        tokio::fs::write(&cert_path, &initial_cert).await.unwrap();
        tokio::fs::write(&key_path, &initial_key).await.unwrap();
        let initial = load_certified_key(&initial_cert, &initial_key).unwrap();
        let initial_der = initial.cert[0].as_ref().to_vec();
        let resolver = Arc::new(ReloadableCertResolver {
            current: RwLock::new(Arc::new(initial)),
        });
        let task = spawn_cert_reload_task(
            cert_path.clone(),
            key_path,
            resolver.clone(),
            CERT_RELOAD_INTERVAL,
        );

        // The startup tick is skipped. A file removed before the first poll
        // must not replace the identity loaded at startup.
        drive_reload_task().await;
        assert_eq!(cert_der(&resolver), initial_der);

        tokio::fs::remove_file(&cert_path).await.unwrap();
        tokio::time::advance(CERT_RELOAD_INTERVAL).await;
        drive_reload_task().await;
        assert_eq!(cert_der(&resolver), initial_der);
        assert!(
            !task.is_finished(),
            "a missing certificate must not stop the reload task"
        );

        task.abort();
    }

    async fn drive_reload_task() {
        // File reads run on the blocking pool, so give that completion a
        // real-time window while the paused runtime keeps polling the task.
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(200);
        while std::time::Instant::now() < deadline {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn tls_incoming_completes_mtls_handshake() {
        install_crypto_provider();

        let (server_cert, server_key) = generate_pem_pair("127.0.0.1");
        let (client_cert, client_key) = generate_pem_pair("client.example.com");

        // Self-signed client cert acts as its own CA for this test.
        let (server_config, _resolver) =
            build_reloadable_server_config(&server_cert, &server_key, &client_cert).unwrap();

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let incoming = TcpIncoming::bind(addr).unwrap();
        let server_addr = incoming.local_addr().unwrap();
        let mut tls_incoming = TlsIncoming::new(
            incoming,
            TlsAcceptor::from(server_config),
            CancellationToken::new(),
        );

        let client_task = tokio::spawn(async move {
            let mut roots = rustls::RootCertStore::empty();
            roots.add_parsable_certificates(parse_certs(&server_cert).unwrap());
            let client_config = rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_client_auth_cert(
                    parse_certs(&client_cert).unwrap(),
                    private_key(&mut &*client_key).unwrap().unwrap(),
                )
                .unwrap();
            let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
            let tcp = TcpStream::connect(server_addr).await.unwrap();
            let server_name = rustls::pki_types::ServerName::try_from("127.0.0.1").unwrap();
            connector.connect(server_name, tcp).await.unwrap();
        });

        use futures::StreamExt;
        let accepted = tls_incoming.next().await;
        assert!(matches!(accepted, Some(Ok(_))), "handshake should succeed");

        client_task.await.unwrap();
    }

    #[tokio::test]
    async fn server_config_accepts_tls13_and_rejects_tls12() {
        install_crypto_provider();

        let (server_cert, server_key) = generate_pem_pair("127.0.0.1");
        let (client_cert, client_key) = generate_pem_pair("client.example.com");
        let (server_config, _resolver) =
            build_reloadable_server_config(&server_cert, &server_key, &client_cert).unwrap();

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let incoming = TcpIncoming::bind(addr).unwrap();
        let server_addr = incoming.local_addr().unwrap();
        let mut tls_incoming = TlsIncoming::new(
            incoming,
            TlsAcceptor::from(server_config),
            CancellationToken::new(),
        );

        let rejected = tokio::spawn(connect_client_version(
            server_addr,
            server_cert.clone(),
            client_cert.clone(),
            client_key.clone(),
            false,
        ));
        use futures::StreamExt;
        let rejected_handshake = tls_incoming.next().await;
        assert!(
            matches!(rejected_handshake, Some(Err(_))),
            "TLS 1.2 must be rejected: {rejected_handshake:?}"
        );
        let _ = rejected.await;

        let accepted = tokio::spawn(connect_client_version(
            server_addr,
            server_cert,
            client_cert,
            client_key,
            true,
        ));
        let accepted_handshake = tls_incoming.next().await;
        assert!(
            matches!(accepted_handshake, Some(Ok(_))),
            "TLS 1.3 must still handshake: {accepted_handshake:?}"
        );
        accepted
            .await
            .unwrap()
            .expect("TLS 1.3 client handshake should succeed");
    }

    async fn connect_client_version(
        server_addr: SocketAddr,
        server_cert: Vec<u8>,
        client_cert: Vec<u8>,
        client_key: Vec<u8>,
        tls13: bool,
    ) -> std::io::Result<()> {
        let versions: &[&rustls::SupportedProtocolVersion] = if tls13 {
            &[&rustls::version::TLS13]
        } else {
            &[&rustls::version::TLS12]
        };
        let mut roots = rustls::RootCertStore::empty();
        roots.add_parsable_certificates(parse_certs(&server_cert).unwrap());
        let client_config = rustls::ClientConfig::builder_with_protocol_versions(versions)
            .with_root_certificates(roots)
            .with_client_auth_cert(
                parse_certs(&client_cert).unwrap(),
                private_key(&mut &*client_key).unwrap().unwrap(),
            )
            .unwrap();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
        let tcp = TcpStream::connect(server_addr).await?;
        let server_name = rustls::pki_types::ServerName::try_from("127.0.0.1").unwrap();
        connector.connect(server_name, tcp).await.map(|_| ())
    }

    #[tokio::test]
    async fn tls_incoming_accepts_the_next_client_after_a_rejected_handshake() {
        install_crypto_provider();

        let (server_cert, server_key) = generate_pem_pair("127.0.0.1");
        let (client_cert, client_key) = generate_pem_pair("client.example.com");
        let (untrusted_cert, untrusted_key) = generate_pem_pair("untrusted.example.com");
        let (server_config, _resolver) =
            build_reloadable_server_config(&server_cert, &server_key, &client_cert).unwrap();

        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let incoming = TcpIncoming::bind(addr).unwrap();
        let server_addr = incoming.local_addr().unwrap();
        let mut tls_incoming = TlsIncoming::new(
            incoming,
            TlsAcceptor::from(server_config),
            CancellationToken::new(),
        );

        let rejected = tokio::spawn(connect_client(
            server_addr,
            server_cert.clone(),
            untrusted_cert,
            untrusted_key,
        ));
        use futures::StreamExt;
        let rejected_handshake = tls_incoming.next().await;
        assert!(
            matches!(rejected_handshake, Some(Err(_))),
            "an untrusted client must not stop the listener: {rejected_handshake:?}"
        );
        let _ = rejected.await;

        let accepted = tokio::spawn(connect_client(
            server_addr,
            server_cert,
            client_cert,
            client_key,
        ));
        let accepted_handshake = tls_incoming.next().await;
        assert!(
            matches!(accepted_handshake, Some(Ok(_))),
            "the next trusted client must still handshake: {accepted_handshake:?}"
        );
        accepted
            .await
            .unwrap()
            .expect("trusted client handshake should succeed");
    }

    async fn connect_client(
        server_addr: SocketAddr,
        server_cert: Vec<u8>,
        client_cert: Vec<u8>,
        client_key: Vec<u8>,
    ) -> std::io::Result<()> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add_parsable_certificates(parse_certs(&server_cert).unwrap());
        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(
                parse_certs(&client_cert).unwrap(),
                private_key(&mut &*client_key).unwrap().unwrap(),
            )
            .unwrap();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
        let tcp = TcpStream::connect(server_addr).await?;
        let server_name = rustls::pki_types::ServerName::try_from("127.0.0.1").unwrap();
        connector.connect(server_name, tcp).await.map(|_| ())
    }
}
