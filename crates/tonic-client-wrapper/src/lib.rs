/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-Apache-2.0
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Note: codegen isn't needed at runtime... dependents should enable this feature in their build-dependencies and disable it otherwise.
#[cfg(feature = "codegen")]
pub mod codegen;
#[cfg(feature = "codegen")]
mod utils;

/// A ConnectionProvider is needed by the generated tonic wrapper to get the actual connection to
/// the server when needed. This is the only thing needed at runtime. This allows
/// tonic-client-wrapper to be agnostic to how connections are actually made to the server.
#[async_trait::async_trait]
pub trait ConnectionProvider<T: Clone>: Send + Sync + std::fmt::Debug + 'static {
    /// Optional observer for unary RPCs, including connection failures. Disabled by default.
    fn rpc_observer(&self) -> Option<&dyn RpcObserver> {
        None
    }

    /// Function which provides a connection.
    ///
    /// The Connection type, T, is the code-generated type from tonic_build that contains all the
    /// RPC methods this crate will be wrapping. It needs to be `Clone` so that it can be used by
    /// multiple clients at once. (Typically you'd use tower's `BoxCloneService` or similar for
    /// this.)
    async fn provide_connection(&self) -> Result<T, tonic::Status>;

    /// Return true if the connection needs to be recreated on the next RPC call. This can be the
    /// case if, for instance, the client certificate on the filesystem has a newer modification
    /// date than the last connection date (indicating we need to use the new client cert.)
    async fn connection_is_stale(&self, last_connected: std::time::SystemTime) -> bool;

    /// Return the server URL for the connection, for debug/logging purposes.
    fn connection_url(&self) -> &str;
}

/// Error returned by observer callbacks. The library logs it and carries on, so an
/// observer failure never changes the outcome of the RPC being observed.
pub type RpcObserverError = Box<dyn std::error::Error + Send + Sync>;

/// Policy supplied by a caller for decoded unary RPC auditing. No metadata is exposed.
/// Callbacks must not block. Payload redaction is the responsibility of the implementor.
///
/// Callbacks report failures by returning `Err` rather than panicking. A failure is
/// logged at `warn` level and the RPC proceeds exactly as if it were not observed.
pub trait RpcObserver: Send + Sync + std::fmt::Debug {
    /// Whether this call should be observed. False avoids payload encoding and callbacks.
    /// Evaluated once at call start; an already-started observation always completes.
    fn enabled(&self) -> bool {
        true
    }

    /// Starts one logical RPC, before lazy connection setup. Names are protobuf names;
    /// `request` is an unredacted protobuf message, not a gRPC frame. If this returns
    /// `Err`, the call is not observed and `RpcObservation::finish` is never invoked.
    fn start(
        &self,
        method: &'static str,
        request_type: &'static str,
        request: &[u8],
        started: std::time::SystemTime,
    ) -> Result<Box<dyn RpcObservation>, RpcObserverError>;
}

/// One in-flight RPC. Implement Drop if cancelled calls should also be recorded.
pub trait RpcObservation: Send {
    /// Span to enter while polling the RPC, including transport context injection.
    fn span(&self) -> tracing::Span;

    /// Records completion. Successful bodies are unredacted protobuf messages;
    /// failures expose only the status code, since status text may contain secrets.
    /// An `Err` is logged and otherwise ignored.
    fn finish(
        &mut self,
        response_type: &'static str,
        response: Option<&[u8]>,
        code: tonic::Code,
        finished: std::time::SystemTime,
    ) -> Result<(), RpcObserverError>;
}

/// Starts auditing only when a provider has an observer; otherwise no encoding is done.
/// An observer error is logged and yields `None`, leaving the call unobserved.
pub fn start_rpc<M: prost::Message>(
    observer: Option<&dyn RpcObserver>,
    method: &'static str,
    request_type: &'static str,
    request: &M,
) -> Option<Box<dyn RpcObservation>> {
    let observer = observer.filter(|observer| observer.enabled())?;
    match observer.start(
        method,
        request_type,
        &request.encode_to_vec(),
        std::time::SystemTime::now(),
    ) {
        Ok(observation) => Some(observation),
        Err(error) => {
            tracing::warn!(method, %error, "RPC observer failed to start; call is unobserved");
            None
        }
    }
}

/// Polls the RPC under the observer's span and reports its unchanged result.
/// An observer error from `finish` is logged and never alters the result.
pub async fn finish_rpc<M: prost::Message, E>(
    mut observation: Option<Box<dyn RpcObservation>>,
    response_type: &'static str,
    future: impl std::future::Future<Output = Result<M, E>>,
    status_code: impl Fn(&E) -> tonic::Code,
) -> Result<M, E> {
    use tracing::Instrument;
    let span = observation
        .as_ref()
        .map(|o| o.span())
        .unwrap_or_else(tracing::Span::none);
    let result = future.instrument(span).await;
    if let Some(observation) = observation.as_mut() {
        let finished = std::time::SystemTime::now();
        let payload = result.as_ref().ok().map(prost::Message::encode_to_vec);
        let code = result.as_ref().err().map_or(tonic::Code::Ok, status_code);
        if let Err(error) = observation.finish(response_type, payload.as_deref(), code, finished) {
            tracing::warn!(response_type, %error, "RPC observer failed to record completion");
        }
    }
    result
}
