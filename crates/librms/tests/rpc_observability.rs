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

#![cfg(feature = "client")]

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper_util::rt::{TokioExecutor, TokioIo};
use librms::client::{
    RetryConfig, RmsApiConfig, RmsTlsClient, RmsTransportLayer, TransportService,
};
use librms::client_config::RmsClientConfig;
use librms::protos::rack_manager as rms;
use librms::protos::rack_manager_v2 as rms_v2;
use librms::protos::rack_manager_v2_client::RackManagerV2ApiClient;
use librms::{RackManagerApi, RmsApi, RpcObservation, RpcObserver, RpcObserverError};
use prost::Message;
use tower::{Service, ServiceExt};

#[derive(Debug, Default)]
struct Capture {
    records: Arc<Mutex<Vec<Record>>>,
    /// Methods whose observation was dropped without `finish` being called.
    cancelled: Arc<Mutex<Vec<&'static str>>>,
}

#[derive(Debug)]
struct Record {
    method: &'static str,
    request_type: &'static str,
    request: Vec<u8>,
    response_type: &'static str,
    response: Option<Vec<u8>>,
    code: tonic::Code,
    started: SystemTime,
    finished: SystemTime,
}

struct Observation {
    capture: Arc<Mutex<Vec<Record>>>,
    cancelled: Arc<Mutex<Vec<&'static str>>>,
    record: Option<Record>,
}

impl Drop for Observation {
    fn drop(&mut self) {
        if let Some(record) = self.record.take() {
            self.cancelled.lock().unwrap().push(record.method);
        }
    }
}

impl RpcObserver for Capture {
    fn start(
        &self,
        method: &'static str,
        request_type: &'static str,
        request: &[u8],
        started: SystemTime,
    ) -> Result<Box<dyn RpcObservation>, RpcObserverError> {
        Ok(Box::new(Observation {
            capture: self.records.clone(),
            cancelled: self.cancelled.clone(),
            record: Some(Record {
                method,
                request_type,
                request: request.to_vec(),
                response_type: "",
                response: None,
                code: tonic::Code::Unknown,
                started,
                finished: started,
            }),
        }))
    }
}

impl RpcObservation for Observation {
    fn span(&self) -> tracing::Span {
        tracing::Span::none()
    }
    fn finish(
        &mut self,
        response_type: &'static str,
        response: Option<&[u8]>,
        code: tonic::Code,
        finished: SystemTime,
    ) -> Result<(), RpcObserverError> {
        let mut record = self.record.take().unwrap();
        record.response_type = response_type;
        record.response = response.map(<[u8]>::to_vec);
        record.code = code;
        record.finished = finished;
        self.capture.lock().unwrap().push(record);
        Ok(())
    }
}

#[derive(Debug)]
struct Disabled {
    started: Arc<AtomicUsize>,
}

impl RpcObserver for Disabled {
    fn enabled(&self) -> bool {
        false
    }
    fn start(
        &self,
        _: &'static str,
        _: &'static str,
        _: &[u8],
        _: SystemTime,
    ) -> Result<Box<dyn RpcObservation>, RpcObserverError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        Err("disabled observer must not be called".into())
    }
}

/// Observer whose callbacks always fail; RPCs must be unaffected.
#[derive(Debug, Default)]
struct Failing {
    fail_start: bool,
    starts: Arc<AtomicUsize>,
    finishes: Arc<AtomicUsize>,
}

struct FailingObservation {
    finishes: Arc<AtomicUsize>,
}

impl RpcObserver for Failing {
    fn start(
        &self,
        _: &'static str,
        _: &'static str,
        _: &[u8],
        _: SystemTime,
    ) -> Result<Box<dyn RpcObservation>, RpcObserverError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        if self.fail_start {
            return Err("start failed".into());
        }
        Ok(Box::new(FailingObservation {
            finishes: self.finishes.clone(),
        }))
    }
}

impl RpcObservation for FailingObservation {
    fn span(&self) -> tracing::Span {
        tracing::Span::none()
    }
    fn finish(
        &mut self,
        _: &'static str,
        _: Option<&[u8]>,
        _: tonic::Code,
        _: SystemTime,
    ) -> Result<(), RpcObserverError> {
        self.finishes.fetch_add(1, Ordering::SeqCst);
        Err("finish failed".into())
    }
}

#[derive(Debug)]
struct InjectHeader;

impl RmsTransportLayer for InjectHeader {
    fn layer(&self, transport: TransportService) -> TransportService {
        tower::service_fn(move |mut request: hyper::Request<tonic::body::Body>| {
            let mut transport = transport.clone();
            request.headers_mut().insert(
                "traceparent",
                "00-11111111111111111111111111111111-2222222222222222-01"
                    .parse()
                    .unwrap(),
            );
            async move { transport.ready().await?.call(request).await }
        })
        .boxed_clone()
    }
}

fn grpc_response<M: Message>(message: M) -> hyper::Response<Full<Bytes>> {
    let body = message.encode_to_vec();
    let mut frame = vec![0];
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend(body);
    hyper::Response::builder()
        .header("content-type", "application/grpc")
        .header("grpc-status", "0")
        .body(Full::new(Bytes::from(frame)))
        .unwrap()
}

#[tokio::test]
async fn real_unary_transport_preserves_payloads_and_observes_v1_v2_and_status() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let service =
                    hyper::service::service_fn(|request: hyper::Request<Incoming>| async move {
                        assert!(request.headers().contains_key("traceparent"));
                        let path = request.uri().path().to_owned();
                        let body = request.into_body().collect().await.unwrap().to_bytes();
                        let response = match path.as_str() {
                            "/rack_manager.RackManager/GetVersion" => {
                                grpc_response(rms::GetVersionResponse {
                                    version: "test-version".to_string(),
                                })
                            }
                            "/rack_manager.RackManager/UpdateSwitchSystemPassword" => {
                                let decoded =
                                    rms::UpdateSwitchSystemPasswordRequest::decode(&body[5..])
                                        .unwrap();
                                assert_eq!(decoded.password, "test-password");
                                hyper::Response::builder()
                                    .header("content-type", "application/grpc")
                                    .header("grpc-status", "7")
                                    .header("grpc-message", "test-password must never be audited")
                                    .body(Full::new(Bytes::new()))
                                    .unwrap()
                            }
                            "/rack_manager_v2.RackManagerV2/ConfigureScaleUpFabricManager" => {
                                grpc_response(rms_v2::ConfigureScaleUpFabricManagerResponse {
                                    job_id: "job-42".to_string(),
                                    ..Default::default()
                                })
                            }
                            "/rack_manager_v2.RackManagerV2/StartSystemValidation" => {
                                grpc_response(rms_v2::StartSystemValidationResponse {
                                    response: Some(rms::NodeBatchResponse {
                                        job_id: "validation-7".to_string(),
                                        ..Default::default()
                                    }),
                                    ..Default::default()
                                })
                            }
                            _ => panic!("unexpected RPC {path}"),
                        };
                        Ok::<_, Infallible>(response)
                    });
                hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(socket), service)
                    .await
                    .unwrap();
            });
        }
    });
    let capture = Arc::new(Capture::default());
    let mut config = RmsClientConfig::new(None, None, None, false);
    config.transport_layer = Some(Arc::new(InjectHeader));
    config.rpc_observer = Some(capture.clone());
    let api_config = RmsApiConfig::new(&url, &config).with_retry_config(RetryConfig {
        retries: 0,
        interval: Duration::ZERO,
    });
    let api = RackManagerApi::new(&api_config);
    let version = api.client.clone().get_version().await.unwrap();
    assert_eq!(version.version, "test-version");
    let request = rms::UpdateSwitchSystemPasswordRequest {
        password: "test-password".to_string(),
        ..Default::default()
    };
    let status = api
        .client
        .update_switch_system_password(request.clone())
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert!(status.message().contains("test-password"));
    let v2 = RackManagerV2ApiClient::new(&api_config);
    let response = v2
        .configure_scale_up_fabric_manager(rms_v2::ConfigureScaleUpFabricManagerRequest::default())
        .await
        .unwrap();
    assert_eq!(response.job_id, "job-42");
    let via_api = api
        .configure_scale_up_fabric_manager_v2(
            rms_v2::ConfigureScaleUpFabricManagerRequest::default(),
        )
        .await
        .unwrap();
    assert_eq!(via_api, response);
    let validation_request = rms_v2::StartSystemValidationRequest::default();
    let validation = api
        .start_system_validation(validation_request.clone())
        .await
        .unwrap();
    assert_eq!(validation.response.as_ref().unwrap().job_id, "validation-7");
    {
        let records = capture.records.lock().unwrap();
        assert_eq!(records.len(), 5); // Readiness probes are transport calls, not logical wrapper calls.
        assert_eq!(records[0].method, "GetVersion");
        assert_eq!(records[0].response_type, "rack_manager.GetVersionResponse");
        assert_eq!(
            rms::GetVersionResponse::decode(records[0].response.as_deref().unwrap()).unwrap(),
            version
        );
        assert_eq!(
            records[1].request_type,
            "rack_manager.UpdateSwitchSystemPasswordRequest"
        );
        assert_eq!(
            rms::UpdateSwitchSystemPasswordRequest::decode(records[1].request.as_slice()).unwrap(),
            request
        );
        assert_eq!(records[1].code, tonic::Code::PermissionDenied);
        assert!(records[1].response.is_none());
        assert_eq!(records[2].method, "ConfigureScaleUpFabricManager");
        assert_eq!(
            records[2].request_type,
            "rack_manager_v2.ConfigureScaleUpFabricManagerRequest"
        );
        assert_eq!(
            records[2].response_type,
            "rack_manager_v2.ConfigureScaleUpFabricManagerResponse"
        );
        assert_eq!(records[2].code, tonic::Code::Ok);
        assert_eq!(
            rms_v2::ConfigureScaleUpFabricManagerResponse::decode(
                records[2].response.as_deref().unwrap()
            )
            .unwrap(),
            response
        );
        // The `RackManagerApi` V2 entry point is observed like the generated wrapper.
        assert_eq!(records[3].method, "ConfigureScaleUpFabricManager");
        assert_eq!(
            records[3].request_type,
            "rack_manager_v2.ConfigureScaleUpFabricManagerRequest"
        );
        assert_eq!(
            records[3].response_type,
            "rack_manager_v2.ConfigureScaleUpFabricManagerResponse"
        );
        assert_eq!(records[3].code, tonic::Code::Ok);
        assert_eq!(
            rms_v2::ConfigureScaleUpFabricManagerResponse::decode(
                records[3].response.as_deref().unwrap()
            )
            .unwrap(),
            via_api
        );
        // The second `RackManagerApi` V2 entry point is observed too.
        assert_eq!(records[4].method, "StartSystemValidation");
        assert_eq!(
            records[4].request_type,
            "rack_manager_v2.StartSystemValidationRequest"
        );
        assert_eq!(
            rms_v2::StartSystemValidationRequest::decode(records[4].request.as_slice()).unwrap(),
            validation_request
        );
        assert_eq!(
            records[4].response_type,
            "rack_manager_v2.StartSystemValidationResponse"
        );
        assert_eq!(records[4].code, tonic::Code::Ok);
        assert_eq!(
            rms_v2::StartSystemValidationResponse::decode(records[4].response.as_deref().unwrap())
                .unwrap(),
            validation
        );
        assert!(capture.cancelled.lock().unwrap().is_empty());
        for record in records.iter() {
            assert!(record.finished >= record.started);
        }
    }
    let disabled_starts = Arc::new(AtomicUsize::new(0));
    config.rpc_observer = Some(Arc::new(Disabled {
        started: disabled_starts.clone(),
    }));
    let unobserved = RackManagerApi::new(&RmsApiConfig::new(&url, &config));
    assert_eq!(unobserved.client.get_version().await.unwrap(), version);
    assert_eq!(capture.records.lock().unwrap().len(), 5);
    assert_eq!(disabled_starts.load(Ordering::SeqCst), 0);
    server.abort();
}

#[tokio::test]
async fn lazy_connection_failure_is_observed_once() {
    let capture = Arc::new(Capture::default());
    let config = RmsClientConfig {
        rpc_observer: Some(capture.clone()),
        ..Default::default()
    };
    // A malformed URL fails before network I/O or retries.
    let api_config = RmsApiConfig::new("http://[invalid", &config);
    let result = RackManagerApi::new(&api_config).client.get_version().await;
    assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
    let records = capture.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].code, tonic::Code::Unavailable);
    assert!(records[0].response.is_none());
}

/// Serves HTTP/2 on a local port; `GetVersion` succeeds, every other RPC never responds.
async fn spawn_server(hang_all: bool) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            tokio::spawn(async move {
                let service =
                    hyper::service::service_fn(move |_: hyper::Request<Incoming>| async move {
                        if hang_all {
                            std::future::pending::<()>().await;
                        }
                        Ok::<_, Infallible>(grpc_response(rms::GetVersionResponse {
                            version: "test-version".to_string(),
                        }))
                    });
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(socket), service)
                    .await;
            });
        }
    });
    (url, server)
}

#[tokio::test]
async fn cancelled_call_drops_observation_without_finish() {
    let (url, server) = spawn_server(true).await;
    let capture = Arc::new(Capture::default());
    let mut config = RmsClientConfig::new(None, None, None, false);
    config.rpc_observer = Some(capture.clone());
    let api_config = RmsApiConfig::new(&url, &config).with_retry_config(RetryConfig {
        retries: 0,
        interval: Duration::ZERO,
    });
    let api = RackManagerApi::new(&api_config);

    let outcome = tokio::time::timeout(Duration::from_millis(200), api.client.get_version()).await;
    assert!(
        outcome.is_err(),
        "call should have been cancelled by the timeout"
    );

    assert!(capture.records.lock().unwrap().is_empty());
    assert_eq!(*capture.cancelled.lock().unwrap(), vec!["GetVersion"]);
    server.abort();
}

#[tokio::test]
async fn observer_errors_never_affect_the_rpc() {
    let (url, server) = spawn_server(false).await;
    for fail_start in [true, false] {
        let failing = Arc::new(Failing {
            fail_start,
            ..Default::default()
        });
        let mut config = RmsClientConfig::new(None, None, None, false);
        config.rpc_observer = Some(failing.clone());
        let api = RackManagerApi::new(&RmsApiConfig::new(&url, &config));

        let version = api.client.get_version().await.unwrap();
        assert_eq!(version.version, "test-version");
        assert_eq!(failing.starts.load(Ordering::SeqCst), 1);
        // A failed start means the call is unobserved, so finish is never reached.
        let expected_finishes = if fail_start { 0 } else { 1 };
        assert_eq!(failing.finishes.load(Ordering::SeqCst), expected_finishes);
    }
    server.abort();
}

#[tokio::test]
async fn idle_connection_preserves_rpc_timeout_and_is_evicted() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());

    // Accept one connection so both RPCs must reuse the same socket.
    let mut server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();

        let service = hyper::service::service_fn(|request: hyper::Request<Incoming>| async move {
            request.into_body().collect().await.unwrap();
            tokio::time::sleep(Duration::from_millis(350)).await;
            Ok::<_, Infallible>(grpc_response(rms::GetVersionResponse::default()))
        });

        let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .serve_connection(TokioIo::new(socket), service)
            .await;
    });

    let config = RmsClientConfig {
        connect_timeout: Some(Duration::from_millis(500)),
        ..Default::default()
    };

    let mut client = RmsTlsClient::new(&config).build_rms_client(url).unwrap();

    client.get_version(rms::GetVersionRequest {}).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let response = tokio::time::timeout(
        Duration::from_secs(2),
        client.get_version(rms::GetVersionRequest {}),
    )
    .await;

    assert!(response.unwrap().is_ok());

    // Verify retention before the pool's 90-second expiry, then cleanup after it.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(88)).await;
    tokio::time::resume();

    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut server)
            .await
            .is_err(),
        "idle connection must remain open before the pool timeout"
    );

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(3)).await;
    tokio::time::resume();

    let closed = tokio::time::timeout(Duration::from_secs(2), &mut server).await;

    server.abort();

    closed
        .expect("unused connection must be evicted without another RPC")
        .unwrap();
}

#[tokio::test]
async fn readiness_probe_respects_the_configured_timeout() {
    let (url, server) = spawn_server(true).await;

    for connect_timeout in [Some(Duration::from_millis(50)), None] {
        let config = RmsClientConfig {
            connect_timeout,
            ..Default::default()
        };

        let api_config = RmsApiConfig::new(&url, &config).with_retry_config(RetryConfig {
            retries: 0,
            interval: Duration::ZERO,
        });

        let started = tokio::time::Instant::now();

        let result = tokio::time::timeout(
            Duration::from_millis(200),
            RmsTlsClient::retry_build_rms(&api_config),
        )
        .await;

        if let Some(timeout) = connect_timeout {
            assert!(
                matches!(&result, Ok(Err(librms::RmsTlsClientError::Connection(_)))),
                "Unexpected readiness result: {:?}",
                result.as_ref().map(|value| value.as_ref().map(|_| ()))
            );

            assert!(started.elapsed() >= timeout);
        } else {
            assert!(
                result.is_err(),
                "None must leave readiness without a deadline"
            );
        }
    }

    server.abort();
}
