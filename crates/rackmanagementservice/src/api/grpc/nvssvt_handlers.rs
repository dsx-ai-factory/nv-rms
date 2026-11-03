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

//! Wires NVSSVT scans into the RMS job framework.
//!
//! `StartSystemValidation` (`RackManagerV2`) takes a `NodeSet` and, for each
//! node, creates a job through [`JobTracker`] and spawns a worker that drives
//! an [`NvssvtRunner`] -- a client for the NVSSVT launcher API running
//! alongside RMS in this pod -- to completion, translating its poll results
//! into job progress/completion/failure. All of a request's per-node jobs
//! are children of one parent batch job, the same `NodeBatchResponse`/parent
//! job convention every other `NodeSet`-addressed RMS RPC uses. Callers track
//! a scan the same way they track any other RMS job -- `RackManager.GetJobStatus`
//! with the job's own `job_id`, or the parent `job_id` to track the whole
//! batch -- so NVSSVT has no status RPC of its own.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use librms::protos::rack_manager as rm;
use librms::protos::rack_manager_v2 as rm_v2;
use nvssvt::runner::{LaunchOutcome, RunState, ScanStatus, StartScanFields};
use nvssvt::{NvssvtConfig, NvssvtRunner};
use serde::Serialize;
use tokio::sync::oneshot;
use tonic::Status;

use crate::domain::rack::{EndpointConfig, EndpointCredentials};
use crate::orchestrator::job_lifecycle::JobError;
use crate::orchestrator::job_tracker::{JobTracker, JobType, RmsJobHandle, job_failure};

use super::conversions::{failed_node_results, flatten_node_info};
use super::server::RackManagerServiceImpl;

/// Interval between NVSSVT status polls while a scan job is running.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// How many consecutive retryable poll failures to ride out before giving up
/// on a scan. NVSSVT runs for far longer than a poll, and the scan keeps
/// running in its own container regardless of what this side sees, so a
/// restarted launcher or a dropped connection should not fail a job that is
/// otherwise healthy. At [`POLL_INTERVAL`] apart, this tolerates roughly
/// half a minute of launcher downtime.
const MAX_POLL_FAILURES: u32 = 5;

/// Rack ID recorded on system validation jobs. NVSSVT scans target a DUT by
/// BMC/host IP, not an RMS rack/node pair, so a fixed rack ID is used purely
/// to namespace these jobs; per-DUT exclusion still comes from the node ID
/// (see [`scan_node_id`]).
const SYSTEM_VALIDATION_RACK_ID: &str = "nvssvt";

/// NVSSVT qualification level every RMS-launched scan runs at. RMS drives one
/// validation profile, so this is fixed here rather than exposed on the RPC.
pub(crate) const SYSTEM_VALIDATION_QUALLEVEL: &str = "NICo";

/// Maps one `NodeSet` entry onto NVSSVT's launch parameters, reusing the same
/// endpoint flattening every other node-addressed RPC uses, so a scan request
/// is shaped like the rest of the API rather than carrying its own bespoke
/// credential fields.
///
/// The BMC endpoint is required -- NVSSVT always scans over the BMC -- while
/// the host OS endpoint is optional. Returns a plain message rather than a
/// `Status`: one node's bad input becomes that node's `NodeOperationResult`,
/// not a failure of the whole batch.
fn node_scan_fields(
    node: &rm::NodeInfo,
    insecure_bmc: bool,
    config_override: &str,
) -> std::result::Result<StartScanFields, String> {
    let flat = flatten_node_info(node).map_err(|e| e.message)?;
    let bmc = flat.bmc_endpoint(insecure_bmc).map_err(|e| e.message)?;
    // NVSSVT only takes a BMC username/password; a session token would be
    // silently dropped and the scan would run on NVSSVT's default credentials.
    if matches!(
        bmc.credentials,
        Some(EndpointCredentials::BmcSessionToken(_))
    ) {
        return Err(
            "NVSSVT requires BMC username/password credentials; session tokens are not supported"
                .to_owned(),
        );
    }
    let host = flat.optional_host_endpoint().map_err(|e| e.message)?;

    // Passed through verbatim: RMS does not interpret NVSSVT's config
    // overrides, and an empty string means "not set" rather than "override
    // with nothing".
    let config_override = (!config_override.is_empty()).then(|| config_override.to_owned());

    Ok(scan_fields_for_endpoints(
        &bmc,
        host.as_ref(),
        config_override,
    ))
}

/// Builds NVSSVT launch fields from a node's BMC (required) and host OS
/// (optional) endpoints, already flattened out of the request's `NodeInfo`.
/// Credentials are read once, at this boundary, into the plaintext NVSSVT
/// itself requires (its CLI takes them as arguments) -- the same
/// plaintext-at-the-boundary pattern `EndpointCredentials::user_pass` is
/// already used for elsewhere (e.g. SFTP / host OS SSH), not a new exposure
/// this change introduces.
fn scan_fields_for_endpoints(
    bmc: &EndpointConfig,
    host: Option<&EndpointConfig>,
    config_override: Option<String>,
) -> StartScanFields {
    let (bmc_user, bmc_pwd) = endpoint_user_pass(bmc);
    let (host_ip, host_user, host_pwd) = match host {
        Some(host) => {
            let (user, pwd) = endpoint_user_pass(host);
            (Some(host.endpoint.ip_address.clone()), user, pwd)
        }
        None => (None, None, None),
    };

    StartScanFields {
        bmc_ip: Some(bmc.endpoint.ip_address.clone()),
        bmc_user,
        bmc_pwd,
        host_ip,
        host_user,
        host_pwd,
        quallevel: vec![SYSTEM_VALIDATION_QUALLEVEL.to_string()],
        config_override,
    }
}

fn endpoint_user_pass(endpoint: &EndpointConfig) -> (Option<String>, Option<String>) {
    endpoint
        .credentials
        .as_ref()
        .and_then(EndpointCredentials::user_pass)
        .map(|(user, pwd)| (Some(user.to_owned()), Some(pwd.to_owned())))
        .unwrap_or((None, None))
}

/// Identifies the DUT a scan targets, for per-node job admission: prefers the
/// BMC IP (what most scans target) and falls back to the host IP so a
/// host-only scan still gets exclusion.
fn scan_node_id(fields: &StartScanFields) -> &str {
    fields
        .bmc_ip
        .as_deref()
        .or(fields.host_ip.as_deref())
        .unwrap_or_default()
}

#[derive(Serialize)]
struct ScanResult {
    return_code: Option<i32>,
    log_dir: String,
    duts: serde_json::Value,
    summary: Option<serde_json::Value>,
    error: Option<String>,
}

fn progress_description(status: &ScanStatus) -> String {
    match status.overall_progress {
        Some(progress) => format!("Running NVSSVT scan ({progress:.0}% complete)"),
        None => "Running NVSSVT scan".to_string(),
    }
}

fn scan_result_json(status: &ScanStatus) -> String {
    let result = ScanResult {
        return_code: status.return_code,
        log_dir: status.log_dir.clone(),
        duts: status.duts.clone(),
        summary: status.summary.clone(),
        error: status.error.clone(),
    };
    serde_json::to_string(&result).unwrap_or_default()
}

/// Drives `run_id` (the launcher's id for this scan, normally identical to
/// `job`'s id) to completion, reporting progress on `job` and sealing it with
/// the final result once NVSSVT exits.
async fn run_scan_job(job: RmsJobHandle, runner: Arc<NvssvtRunner>, run_id: String) {
    let mut consecutive_failures = 0u32;
    loop {
        let status = match runner.status(&run_id).await {
            Ok(status) => {
                consecutive_failures = 0;
                status
            }
            Err(err) if err.is_retryable() && consecutive_failures < MAX_POLL_FAILURES => {
                consecutive_failures += 1;
                tracing::warn!(
                    run_id = %run_id,
                    attempt = consecutive_failures,
                    max_attempts = MAX_POLL_FAILURES,
                    error = %err,
                    "NVSSVT status poll failed; the scan itself is still running, retrying"
                );
                if poll_interval_elapsed_or_cancelled(&job).await {
                    job.fail(job_failure(
                        JobError::Other,
                        format!("NVSSVT scan {run_id} cancelled"),
                    ));
                    return;
                }
                continue;
            }
            Err(err) => {
                job.fail(job_failure(
                    JobError::Internal,
                    format!("failed to poll NVSSVT scan {run_id}: {err}"),
                ));
                return;
            }
        };

        match status.state {
            RunState::Starting | RunState::Running | RunState::Unknown => {
                job.progress(progress_description(&status));
            }
            RunState::Completed => {
                job.complete("NVSSVT scan completed", scan_result_json(&status));
                return;
            }
            RunState::Failed => {
                let message = status
                    .error
                    .clone()
                    .unwrap_or_else(|| "NVSSVT scan failed".to_string());
                job.fail(
                    job_failure(JobError::Other, message)
                        .with_result_json(scan_result_json(&status)),
                );
                return;
            }
        }

        if poll_interval_elapsed_or_cancelled(&job).await {
            job.fail(job_failure(
                JobError::Other,
                format!("NVSSVT scan {run_id} cancelled"),
            ));
            return;
        }
    }
}

/// Sleeps until the next poll is due. Returns `true` if the scan was
/// cancelled while waiting, which the caller -- which owns `job`, and so is
/// the only one who can seal it -- reports as a failure.
async fn poll_interval_elapsed_or_cancelled(job: &RmsJobHandle) -> bool {
    let cancel = job.cancellation_token();
    tokio::select! {
        _ = tokio::time::sleep(POLL_INTERVAL) => false,
        _ = cancel.cancelled() => true,
    }
}

/// Spawns `job`, already admitted by the caller, as a supervised worker that
/// launches the NVSSVT scan described by `fields` and polls it to completion.
///
/// The launch happens inside the worker, not in the caller, so the job
/// outlives the RPC: a caller that disconnects mid-launch cannot drop the
/// handle, which would fail the job and release the DUT while the launcher
/// keeps the scan running. The returned receiver yields the launch outcome
/// for the caller to report; if the caller is gone, the worker carries on.
///
/// The RMS job id is handed to the launcher as the NVSSVT run id, so the two
/// sides name a scan identically and neither keeps a mapping. The launcher
/// echoes the id it used, and that echo -- not the job id -- is what the
/// worker polls, so a launcher that ever assigns its own id still works.
fn spawn_system_validation(
    job_tracker: &Arc<JobTracker>,
    runner: Arc<NvssvtRunner>,
    job: RmsJobHandle,
    fields: StartScanFields,
) -> oneshot::Receiver<std::result::Result<(), String>> {
    let (launched_tx, launched_rx) = oneshot::channel();
    job_tracker
        .spawn_job(job, move |job| async move {
            let job_id = job.id().to_string();
            let LaunchOutcome {
                run_id,
                pid,
                log_dir,
                started_at: _,
            } = match runner.launch(Some(&job_id), fields).await {
                Ok(outcome) => outcome,
                Err(err) => {
                    let message = err.to_string();
                    job.fail(job_failure(JobError::Internal, &message));
                    let _ = launched_tx.send(Err(message));
                    return;
                }
            };

            // The process id and log directory belong to the launcher's
            // container, so they are of no use to a caller and are not in the
            // response. They are still what an operator needs to find a scan
            // on the NVSSVT side, so they are recorded here instead.
            tracing::info!(
                job_id = %job_id,
                run_id = %run_id,
                nvssvt_pid = pid,
                nvssvt_log_dir = %log_dir,
                "NVSSVT scan launched"
            );
            let _ = launched_tx.send(Ok(()));

            run_scan_job(job, runner, run_id).await;
        })
        .detach();
    launched_rx
}

impl RackManagerServiceImpl {
    /// The main handler for the `RackManagerV2.StartSystemValidation` RPC.
    ///
    /// The NVSSVT client is stateless (an HTTP client bound to the in-pod
    /// sidecar at its fixed default address), so it is built per request
    /// rather than held on the service.
    pub(crate) async fn handle_start_system_validation(
        &self,
        req: tonic::Request<rm_v2::StartSystemValidationRequest>,
    ) -> std::result::Result<tonic::Response<rm_v2::StartSystemValidationResponse>, Status> {
        let runner = NvssvtRunner::new(NvssvtConfig::default())
            .map(Arc::new)
            .map_err(|e| {
                Status::internal(format!("failed to build the NVSSVT launcher client: {e}"))
            })?;
        let response = start_system_validation(
            &self.job_tracker,
            &runner,
            self.insecure_bmc,
            req.into_inner(),
        )
        .await;
        Ok(tonic::Response::new(response))
    }
}

/// Launches an independent NVSSVT scan, as its own asynchronous
/// `JobType::SystemValidation` job, for every node in `request`.
///
/// Node-level problems (bad input, a busy DUT, an unreachable launcher) are
/// reported per node in the response, the same as every other
/// `NodeSet`-addressed RMS RPC. The batch only fails as a whole when the
/// request carries no nodes at all, or the tracker cannot create the parent
/// batch job. Per-node results retain the request's `node_id`, including
/// refusals for aliases that share the same DUT BMC address.
async fn start_system_validation(
    job_tracker: &Arc<JobTracker>,
    runner: &Arc<NvssvtRunner>,
    insecure_bmc: bool,
    request: rm_v2::StartSystemValidationRequest,
) -> rm_v2::StartSystemValidationResponse {
    let devices: Vec<rm::NodeInfo> = request.nodes.map(|nodes| nodes.nodes).unwrap_or_default();

    if devices.is_empty() {
        return rm_v2::StartSystemValidationResponse {
            response: Some(batch_response(
                String::new(),
                0,
                0,
                Vec::new(),
                "No devices specified in request",
            )),
            jobs: Vec::new(),
        };
    }
    let total_nodes = devices.len() as u32;

    // Validate before admission so invalid entries reserve no child job.
    // DUT BMC addresses identify admission targets; caller IDs identify results.
    let mut node_results = Vec::new();
    let mut targets = Vec::new();
    let mut target_ids = Vec::new();

    for device in devices {
        let node_id = device.node_id.clone();
        match node_scan_fields(&device, insecure_bmc, &request.config_override) {
            Ok(fields) => {
                let scan_key = scan_node_id(&fields).to_owned();

                target_ids.push((scan_key.clone(), node_id.clone()));

                targets.push((
                    SYSTEM_VALIDATION_RACK_ID.to_owned(),
                    scan_key,
                    (node_id, fields),
                ));
            }
            Err(message) => node_results.push(rm::NodeOperationResult {
                node_id,
                status: rm::ReturnCode::Failure.into(),
                error_message: message,
            }),
        }
    }

    let Some(parent_job_id) =
        job_tracker.create_parent_job(SYSTEM_VALIDATION_RACK_ID, JobType::SystemValidation)
    else {
        return rm_v2::StartSystemValidationResponse {
            response: Some(batch_response(
                String::new(),
                0,
                0,
                Vec::new(),
                "failed to create parent system validation job",
            )),
            jobs: Vec::new(),
        };
    };

    let (admitted, mut rejected) = job_tracker.create_batch_jobs(
        &parent_job_id,
        JobType::SystemValidation,
        targets,
        |_fields: &(String, StartScanFields)| Ok::<(), String>(()),
    );

    // Only a key's first occurrence can be admitted. Remove it once so the
    // remaining caller IDs match the tracker's request-ordered refusals.
    let mut admitted_keys: HashSet<_> = admitted
        .iter()
        .map(|((_, fields), _, _)| scan_node_id(fields))
        .collect();

    let rejected_ids = target_ids
        .into_iter()
        .filter(|(key, _)| !admitted_keys.remove(key.as_str()));

    for (rejected, (_, node_id)) in rejected.iter_mut().zip(rejected_ids) {
        rejected.node_id = node_id;
    }

    node_results.extend(failed_node_results(rejected));

    if admitted.is_empty() {
        job_tracker.mark_failed_message(&parent_job_id, "No system validation jobs created");
    }

    // Every admitted job is spawned before any launch is awaited, so none of
    // them depends on this RPC staying alive.
    let launches: Vec<_> = admitted
        .into_iter()
        .map(|((node_id, fields), job, ())| {
            let job_id = job.id().to_string();
            let launched = spawn_system_validation(job_tracker, runner.clone(), job, fields);
            (node_id, job_id, launched)
        })
        .collect();

    let mut jobs = Vec::new();
    for (node_id, job_id, launched) in launches {
        let launched = launched.await.unwrap_or_else(|_| {
            Err("system validation job ended before the launch completed".to_owned())
        });
        match launched {
            Ok(()) => {
                jobs.push(rm_v2::NodeSystemValidationJobInfo {
                    node_id: node_id.clone(),
                    job_id,
                });
                node_results.push(rm::NodeOperationResult {
                    node_id,
                    status: rm::ReturnCode::Success.into(),
                    error_message: String::new(),
                });
            }
            Err(err) => node_results.push(rm::NodeOperationResult {
                node_id,
                status: rm::ReturnCode::Failure.into(),
                error_message: err,
            }),
        }
    }

    let response = batch_response(
        parent_job_id,
        total_nodes,
        jobs.len() as u32,
        node_results,
        "Created system validation jobs. Use GetJobStatus to track completion.",
    );

    rm_v2::StartSystemValidationResponse {
        response: Some(response),
        jobs,
    }
}

/// The `NodeBatchResponse` for a batch of `total_nodes`, `successful_nodes`
/// of which launched. Succeeds only when every node did; an empty batch is a
/// failure. `job_id` is the parent job, or empty when none was created.
fn batch_response(
    job_id: String,
    total_nodes: u32,
    successful_nodes: u32,
    node_results: Vec<rm::NodeOperationResult>,
    message: &str,
) -> rm::NodeBatchResponse {
    let failed_nodes = total_nodes - successful_nodes;
    let status = if total_nodes > 0 && failed_nodes == 0 {
        rm::ReturnCode::Success
    } else {
        rm::ReturnCode::Failure
    };
    rm::NodeBatchResponse {
        status: status.into(),
        message: message.to_owned(),
        node_results,
        job_id,
        stats: Some(rm::NodeOperationStats {
            total_nodes,
            successful_nodes,
            failed_nodes,
        }),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    //! Exercises NVSSVT scans end-to-end through the RMS job framework,
    //! against a stubbed launcher API rather than a real NVSSVT sidecar, so
    //! these tests are hermetic and fast.
    //!
    //! There is no `GetScanStatus`/status RPC of its own: progress/completion
    //! is observed through the shared `JobTracker`, the same way
    //! firmware/switch jobs are.

    use std::sync::Arc;
    use std::time::Instant;

    use serde_json::json;
    use wiremock::matchers::{method, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::orchestrator::job_lifecycle::JobState;
    use crate::orchestrator::job_tracker::JobTracker;

    /// Stubs the launcher's two endpoints: every launch is accepted, and
    /// every poll reports `status_body`, so a scan reaches a terminal state
    /// on the job worker's first poll.
    pub(crate) async fn mock_launcher(status_body: serde_json::Value) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path_regex("^/runs$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "run_id": "abc123",
                "pid": 4242,
                "log_dir": "/opt/nvssvt/api_runs/abc123",
                "started_at": "2026-09-23T10:00:00+00:00",
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path_regex("^/runs/.+$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(status_body))
            .mount(&server)
            .await;
        server
    }

    pub(crate) fn completed_status() -> serde_json::Value {
        json!({
            "run_id": "abc123",
            "pid": 4242,
            "state": "completed",
            "return_code": 0,
            "started_at": "2026-09-23T10:00:00+00:00",
            "log_dir": "/opt/nvssvt/api_runs/abc123/NVSSVT_Logs_1",
            "overall_progress": 100.0,
            "duts": {"10.0.0.5": {"OVERALL_NVSSVT_PROGRESS": 100.0}},
            "summary": {"pass": 1, "fail": 0},
            "error": null,
        })
    }

    fn failed_status() -> serde_json::Value {
        json!({
            "run_id": "abc123",
            "pid": 4242,
            "state": "failed",
            "return_code": 1,
            "started_at": "2026-09-23T10:00:00+00:00",
            "log_dir": "/opt/nvssvt/api_runs/abc123",
            "overall_progress": null,
            "duts": {},
            "summary": null,
            "error": "simulated failure",
        })
    }

    /// How many scans the stubbed launcher was asked to start.
    pub(crate) async fn launch_count(server: &MockServer) -> usize {
        server
            .received_requests()
            .await
            .unwrap_or_default()
            .iter()
            .filter(|req| req.method == wiremock::http::Method::Post)
            .count()
    }

    /// The JSON body of the first launch the stubbed launcher received.
    pub(crate) async fn launch_body(server: &MockServer) -> serde_json::Value {
        let requests = server.received_requests().await.unwrap_or_default();
        let launch = requests
            .iter()
            .find(|req| req.method == wiremock::http::Method::Post)
            .expect("a scan should have been launched");
        serde_json::from_slice(&launch.body).expect("launch body should be JSON")
    }

    /// A `NodeInfo` addressing `bmc_ip` over Redfish with username/password
    /// credentials -- the shape every scan request node takes.
    fn scan_node(node_id: &str, bmc_ip: &str) -> librms::protos::rack_manager::NodeInfo {
        librms::protos::rack_manager::NodeInfo {
            node_id: node_id.to_owned(),
            rack_id: "rack-1".to_owned(),
            bmc_endpoint: Some(bmc_endpoint(bmc_ip, "admin", "password")),
            ..Default::default()
        }
    }

    /// A single-node `StartSystemValidationRequest` -- the shape most tests need.
    fn scan_request(bmc_ip: &str) -> rm_v2::StartSystemValidationRequest {
        scan_request_for_nodes(vec![scan_node("compute-1", bmc_ip)])
    }

    fn scan_request_for_nodes(
        nodes: Vec<librms::protos::rack_manager::NodeInfo>,
    ) -> rm_v2::StartSystemValidationRequest {
        rm_v2::StartSystemValidationRequest {
            nodes: Some(librms::protos::rack_manager::NodeSet { nodes }),
            ..Default::default()
        }
    }

    fn bmc_endpoint(ip: &str, user: &str, pwd: &str) -> librms::protos::rack_manager::Endpoint {
        use librms::protos::rack_manager as rm;
        rm::Endpoint {
            interface: Some(rm::NetworkInterface {
                ip_address: ip.to_owned(),
                mac_address: "00:11:22:33:44:55".to_owned(),
                host_name: None,
            }),
            port: 443,
            credentials: Some(rm::Credentials {
                auth: Some(rm::credentials::Auth::UserPass(rm::UsernamePassword {
                    username: user.to_owned(),
                    password: pwd.to_owned(),
                })),
            }),
        }
    }

    pub(crate) fn runner_for(server: &MockServer) -> Arc<NvssvtRunner> {
        let config = NvssvtConfig::default().with_base_url(server.uri());
        Arc::new(NvssvtRunner::new(config).expect("build runner"))
    }

    /// A runner pointed at a port nothing is listening on, standing in for a
    /// launcher sidecar that is down.
    pub(crate) fn unreachable_runner() -> Arc<NvssvtRunner> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        drop(listener);
        Arc::new(
            NvssvtRunner::new(
                NvssvtConfig::new("127.0.0.1", port)
                    .with_request_timeout(std::time::Duration::from_secs(2)),
            )
            .expect("build runner"),
        )
    }

    /// What `handle_start_system_validation` hands to
    /// `start_system_validation`, but with the runner pointed at a test's own
    /// stubbed launcher instead of the in-pod default.
    struct TestService {
        job_tracker: Arc<JobTracker>,
        runner: Arc<NvssvtRunner>,
    }

    impl TestService {
        async fn start(
            &self,
            request: rm_v2::StartSystemValidationRequest,
        ) -> rm_v2::StartSystemValidationResponse {
            start_system_validation(&self.job_tracker, &self.runner, true, request).await
        }
    }

    fn test_service(runner: Arc<NvssvtRunner>) -> TestService {
        TestService {
            job_tracker: Arc::new(JobTracker::new()),
            runner,
        }
    }

    /// Polls the job tracker until `job_id` reaches a terminal state, bounded
    /// so a bug that leaves a job stuck fails the test instead of hanging it.
    async fn wait_for_terminal_job(
        service: &TestService,
        job_id: &str,
    ) -> crate::orchestrator::job_tracker::JobInfo {
        // Comfortably longer than POLL_INTERVAL: these tests run alongside
        // the rest of the lib's suite, which adds scheduling slack on a
        // loaded machine.
        let deadline = Instant::now() + POLL_INTERVAL * 3;
        loop {
            let info = service
                .job_tracker
                .get_job(job_id)
                .expect("job should be tracked");
            if matches!(info.state, JobState::Completed | JobState::Failed) {
                return info;
            }
            assert!(
                Instant::now() < deadline,
                "scan job did not reach a terminal state in time"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn start_system_validation_completes_through_the_job_tracker() {
        let server = mock_launcher(completed_status()).await;
        let service = test_service(runner_for(&server));

        let response = service.start(scan_request("10.0.0.5")).await;

        let batch = response.response.expect("batch response");
        assert_eq!(batch.status, rm::ReturnCode::Success as i32);
        assert!(!batch.job_id.is_empty(), "parent job id should be set");
        assert_eq!(response.jobs.len(), 1);
        assert_eq!(response.jobs[0].node_id, "compute-1");
        let job_id = &response.jobs[0].job_id;
        assert!(!job_id.is_empty());

        let info = wait_for_terminal_job(&service, job_id).await;
        assert_eq!(info.state, JobState::Completed);
        assert!(info.result_json.contains("\"pass\":1"));
        assert!(info.result_json.contains("OVERALL_NVSSVT_PROGRESS"));
    }

    #[tokio::test]
    async fn start_system_validation_runs_an_independent_scan_per_node() {
        let server = mock_launcher(completed_status()).await;
        let service = test_service(runner_for(&server));

        let response = service
            .start(scan_request_for_nodes(vec![
                scan_node("compute-1", "10.0.0.5"),
                scan_node("compute-2", "10.0.0.6"),
            ]))
            .await;

        let batch = response.response.expect("batch response");
        assert_eq!(batch.status, rm::ReturnCode::Success as i32);
        assert_eq!(
            batch.stats,
            Some(rm::NodeOperationStats {
                total_nodes: 2,
                successful_nodes: 2,
                failed_nodes: 0,
            })
        );
        assert_eq!(response.jobs.len(), 2);
        assert_ne!(
            response.jobs[0].job_id, response.jobs[1].job_id,
            "each node should get its own scan job"
        );
        assert_eq!(launch_count(&server).await, 2);

        for job in &response.jobs {
            let info = wait_for_terminal_job(&service, &job.job_id).await;
            assert_eq!(info.state, JobState::Completed);
        }
    }

    #[tokio::test]
    async fn a_duplicate_dut_in_the_same_request_fails_only_the_second_entry() {
        let server = mock_launcher(completed_status()).await;
        let service = test_service(runner_for(&server));

        let response = service
            .start(scan_request_for_nodes(vec![
                scan_node("compute-1", "10.0.0.5"),
                scan_node("compute-1-again", "10.0.0.5"),
            ]))
            .await;

        assert_eq!(response.jobs.len(), 1, "only the first entry should launch");
        assert_eq!(launch_count(&server).await, 1);
        let batch = response.response.expect("batch response");

        assert_eq!(
            batch
                .node_results
                .iter()
                .find(|result| result.status == rm::ReturnCode::Failure as i32)
                .map(|result| result.node_id.as_str()),
            Some("compute-1-again")
        );

        assert_eq!(
            batch.stats,
            Some(rm::NodeOperationStats {
                total_nodes: 2,
                successful_nodes: 1,
                failed_nodes: 1,
            })
        );
    }

    #[tokio::test]
    async fn a_mixed_batch_preserves_caller_ids_for_busy_and_duplicate_refusals() {
        let server = mock_launcher(completed_status()).await;
        let service = test_service(runner_for(&server));

        let _busy = service
            .job_tracker
            .create_job(
                SYSTEM_VALIDATION_RACK_ID,
                "10.0.0.9",
                JobType::SystemValidation,
            )
            .expect("reserve busy DUT");

        let response = service
            .start(scan_request_for_nodes(vec![
                scan_node("busy-alias", "10.0.0.9"),
                scan_node("fresh", "10.0.0.10"),
                scan_node("fresh-alias", "10.0.0.10"),
            ]))
            .await;

        assert_eq!(response.jobs.len(), 1);
        assert_eq!(response.jobs[0].node_id, "fresh");
        assert_eq!(launch_count(&server).await, 1);

        let batch = response.response.expect("batch response");

        let results: Vec<_> = batch
            .node_results
            .iter()
            .map(|result| (result.node_id.as_str(), result.status))
            .collect();

        assert_eq!(
            results,
            [
                ("busy-alias", rm::ReturnCode::Failure as i32),
                ("fresh-alias", rm::ReturnCode::Failure as i32),
                ("fresh", rm::ReturnCode::Success as i32),
            ]
        );

        wait_for_terminal_job(&service, &response.jobs[0].job_id).await;
    }

    #[tokio::test]
    async fn a_partial_capacity_refusal_preserves_the_caller_id() {
        let server = mock_launcher(completed_status()).await;

        let service = TestService {
            runner: runner_for(&server),
            job_tracker: Arc::new(
                JobTracker::builder()
                    .max_tracked_jobs(2)
                    .build()
                    .expect("build capacity-limited tracker"),
            ),
        };

        let response = service
            .start(scan_request_for_nodes(vec![
                scan_node("compute-1", "10.0.0.5"),
                scan_node("compute-2", "10.0.0.6"),
            ]))
            .await;

        assert_eq!(response.jobs.len(), 1);
        assert_eq!(response.jobs[0].node_id, "compute-1");
        assert_eq!(launch_count(&server).await, 1);

        let batch = response.response.expect("batch response");

        let failure = batch
            .node_results
            .iter()
            .find(|result| result.status == rm::ReturnCode::Failure as i32)
            .expect("capacity refusal");

        assert_eq!(batch.node_results.len(), 2);
        assert_eq!(failure.node_id, "compute-2");
        assert!(failure.error_message.contains("capacity"));

        wait_for_terminal_job(&service, &response.jobs[0].job_id).await;
    }

    #[tokio::test]
    async fn every_scan_is_launched_at_the_fixed_qualification_level() {
        let server = mock_launcher(completed_status()).await;
        let service = test_service(runner_for(&server));

        service.start(scan_request("10.0.0.5")).await;

        let body = launch_body(&server).await;
        assert_eq!(body["quallevel"], json!([SYSTEM_VALIDATION_QUALLEVEL]));
        assert_eq!(body["bmc_user"], "admin");
        // Credentials the caller left empty stay absent, so NVSSVT falls back
        // to its own CLI defaults rather than receiving empty strings.
        assert!(body.get("host_ip").is_none(), "{body}");
    }

    #[tokio::test]
    async fn the_rms_job_id_is_used_as_the_nvssvt_run_id() {
        let server = mock_launcher(completed_status()).await;
        let service = test_service(runner_for(&server));

        let response = service.start(scan_request("10.0.0.5")).await;

        assert_eq!(
            launch_body(&server).await["run_id"],
            response.jobs[0].job_id
        );
    }

    #[tokio::test]
    async fn a_transient_poll_failure_does_not_fail_the_scan() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path_regex("^/runs$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "run_id": "abc123",
                "pid": 4242,
                "log_dir": "/opt/nvssvt/api_runs/abc123",
                "started_at": "2026-09-23T10:00:00+00:00",
            })))
            .mount(&server)
            .await;
        // The launcher blips on the first poll -- a restart, a dropped
        // keep-alive connection -- then answers normally. The scan itself
        // never stopped running, so the job must not be failed over it.
        Mock::given(method("GET"))
            .and(path_regex("^/runs/.+$"))
            .respond_with(ResponseTemplate::new(503).set_body_string("launcher restarting"))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path_regex("^/runs/.+$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(completed_status()))
            .with_priority(2)
            .mount(&server)
            .await;
        let service = test_service(runner_for(&server));

        let response = service.start(scan_request("10.0.0.5")).await;

        let info = wait_for_terminal_job(&service, &response.jobs[0].job_id).await;
        assert_eq!(info.state, JobState::Completed, "{}", info.error_message);
    }

    #[tokio::test]
    async fn a_scan_the_launcher_has_lost_fails_without_retrying() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path_regex("^/runs$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "run_id": "abc123",
                "pid": 4242,
                "log_dir": "/opt/nvssvt/api_runs/abc123",
                "started_at": "2026-09-23T10:00:00+00:00",
            })))
            .mount(&server)
            .await;
        // A 404 is terminal: retrying will not make a run the launcher has
        // never heard of reappear.
        Mock::given(method("GET"))
            .and(path_regex("^/runs/.+$"))
            .respond_with(ResponseTemplate::new(404).set_body_string("Unknown run_id"))
            .expect(1)
            .mount(&server)
            .await;
        let service = test_service(runner_for(&server));

        let response = service.start(scan_request("10.0.0.5")).await;

        let info = wait_for_terminal_job(&service, &response.jobs[0].job_id).await;
        assert_eq!(info.state, JobState::Failed);
        assert!(info.error_message.contains("unknown NVSSVT run id"));
    }

    #[tokio::test]
    async fn start_system_validation_reports_failure_through_the_job_tracker() {
        let server = mock_launcher(failed_status()).await;
        let service = test_service(runner_for(&server));

        let response = service.start(scan_request("10.0.0.5")).await;

        let info = wait_for_terminal_job(&service, &response.jobs[0].job_id).await;
        assert_eq!(info.state, JobState::Failed);
        assert!(info.error_message.contains("simulated failure"));
    }

    #[tokio::test]
    async fn a_caller_that_gives_up_mid_launch_does_not_orphan_the_scan() {
        let server = MockServer::start().await;
        // The launcher accepts the run but is slow to answer, so the caller's
        // deadline expires while the launch is still in flight.
        Mock::given(method("POST"))
            .and(path_regex("^/runs$"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({
                        "run_id": "abc123",
                        "pid": 4242,
                        "log_dir": "/opt/nvssvt/api_runs/abc123",
                        "started_at": "2026-09-23T10:00:00+00:00",
                    }))
                    .set_delay(Duration::from_millis(500)),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path_regex("^/runs/.+$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "run_id": "abc123",
                "pid": 4242,
                "state": "running",
                "return_code": null,
                "started_at": "2026-09-23T10:00:00+00:00",
                "log_dir": "/opt/nvssvt/api_runs/abc123",
                "overall_progress": 10.0,
                "duts": {},
                "summary": null,
                "error": null,
            })))
            .mount(&server)
            .await;
        let service = test_service(runner_for(&server));

        let gave_up = tokio::time::timeout(
            Duration::from_millis(100),
            service.start(scan_request("10.0.0.7")),
        )
        .await;
        assert!(gave_up.is_err(), "the launch should still be in flight");

        // The job outlived the caller and still holds the DUT, so a retry is
        // refused instead of starting a second NVSSVT run against it.
        let retry = service.start(scan_request("10.0.0.7")).await;
        assert!(retry.jobs.is_empty());
        let message = &retry.response.expect("batch response").node_results[0].error_message;
        assert!(message.contains("already in progress"), "{message}");
        let job_id = message
            .rsplit("job_id ")
            .next()
            .and_then(|id| id.strip_suffix(')'))
            .expect("the refusal should name the active job");

        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(launch_count(&server).await, 1);
        let info = service
            .job_tracker
            .get_job(job_id)
            .expect("job should be tracked");
        assert_ne!(info.state, JobState::Failed, "{}", info.error_message);
    }

    #[tokio::test]
    async fn a_second_scan_against_a_busy_dut_fails_only_that_node() {
        let server = mock_launcher(json!({
            "run_id": "abc123",
            "pid": 4242,
            "state": "running",
            "return_code": null,
            "started_at": "2026-09-23T10:00:00+00:00",
            "log_dir": "/opt/nvssvt/api_runs/abc123",
            "overall_progress": 10.0,
            "duts": {},
            "summary": null,
            "error": null,
        }))
        .await;
        let service = test_service(runner_for(&server));

        let _first = service.start(scan_request("10.0.0.9")).await;

        let second = service.start(scan_request("10.0.0.9")).await;

        assert!(
            second.jobs.is_empty(),
            "no job should be created for a busy DUT"
        );
        let batch = second.response.expect("batch response");
        assert_eq!(batch.status, rm::ReturnCode::Failure as i32);
        assert_eq!(batch.node_results.len(), 1);
        assert_eq!(batch.node_results[0].node_id, "compute-1");
        assert_eq!(batch.node_results[0].status, rm::ReturnCode::Failure as i32);
        assert!(
            batch.node_results[0]
                .error_message
                .contains("already in progress"),
            "{}",
            batch.node_results[0].error_message
        );
    }

    #[tokio::test]
    async fn an_empty_node_set_is_rejected_without_creating_a_job() {
        let server = mock_launcher(completed_status()).await;
        let service = test_service(runner_for(&server));

        let response = service
            .start(rm_v2::StartSystemValidationRequest {
                nodes: None,
                ..Default::default()
            })
            .await;

        let batch = response.response.expect("batch response");
        assert_eq!(batch.status, rm::ReturnCode::Failure as i32);
        assert!(batch.job_id.is_empty(), "no parent job should be created");
        assert!(response.jobs.is_empty());
        assert_eq!(launch_count(&server).await, 0);
    }

    #[tokio::test]
    async fn a_node_without_a_bmc_endpoint_fails_only_that_node() {
        let server = mock_launcher(completed_status()).await;
        let service = test_service(runner_for(&server));

        // NVSSVT always scans over the BMC, so a host-only node cannot be
        // scanned -- catch that here rather than launching a scan NVSSVT
        // will reject.
        let response = service
            .start(scan_request_for_nodes(vec![
                librms::protos::rack_manager::NodeInfo {
                    node_id: "compute-1".to_owned(),
                    rack_id: "rack-1".to_owned(),
                    ..Default::default()
                },
            ]))
            .await;

        assert!(response.jobs.is_empty());
        assert_eq!(launch_count(&server).await, 0);
        let batch = response.response.expect("batch response");
        assert_eq!(batch.status, rm::ReturnCode::Failure as i32);
        assert_eq!(batch.node_results.len(), 1);
        assert_eq!(batch.node_results[0].node_id, "compute-1");
        assert_eq!(batch.node_results[0].status, rm::ReturnCode::Failure as i32);
    }

    #[tokio::test]
    async fn a_bmc_with_session_token_credentials_fails_only_that_node() {
        let server = mock_launcher(completed_status()).await;
        let service = test_service(runner_for(&server));

        let mut node = scan_node("compute-1", "10.0.0.5");
        node.bmc_endpoint.as_mut().unwrap().credentials =
            Some(librms::protos::rack_manager::Credentials {
                auth: Some(
                    librms::protos::rack_manager::credentials::Auth::SessionToken(
                        "token".to_owned(),
                    ),
                ),
            });

        // NVSSVT cannot use a session token, so launching anyway would run the
        // scan on NVSSVT's default credentials instead of the caller's.
        let response = service.start(scan_request_for_nodes(vec![node])).await;

        assert!(response.jobs.is_empty());
        assert_eq!(launch_count(&server).await, 0);
        let batch = response.response.expect("batch response");
        assert_eq!(batch.node_results.len(), 1);
        assert_eq!(batch.node_results[0].status, rm::ReturnCode::Failure as i32);
        assert!(
            batch.node_results[0]
                .error_message
                .contains("session tokens"),
            "{}",
            batch.node_results[0].error_message
        );
    }

    #[tokio::test]
    async fn the_config_override_is_forwarded_to_nvssvt() {
        let server = mock_launcher(completed_status()).await;
        let service = test_service(runner_for(&server));

        let mut request = scan_request("10.0.0.5");
        request.config_override = "power_limit=500".to_owned();

        service.start(request).await;

        assert_eq!(
            launch_body(&server).await["config_override"],
            "power_limit=500"
        );
    }

    #[tokio::test]
    async fn an_empty_config_override_is_omitted_rather_than_sent_blank() {
        let server = mock_launcher(completed_status()).await;
        let service = test_service(runner_for(&server));

        service.start(scan_request("10.0.0.5")).await;

        // proto3 has no unset string, so an empty one must not reach NVSSVT as
        // an explicit empty --config-override.
        let body = launch_body(&server).await;
        assert!(body.get("config_override").is_none(), "{body}");
    }

    #[tokio::test]
    async fn the_host_endpoint_is_forwarded_when_supplied() {
        let server = mock_launcher(completed_status()).await;
        let service = test_service(runner_for(&server));

        let mut request = scan_request("10.0.0.5");
        request.nodes.as_mut().unwrap().nodes[0].host_endpoint =
            Some(bmc_endpoint("10.0.1.5", "root", "hostpw"));

        service.start(request).await;

        let body = launch_body(&server).await;
        assert_eq!(body["host_ip"], "10.0.1.5");
        assert_eq!(body["host_user"], "root");
        assert_eq!(body["host_pwd"], "hostpw");
    }

    #[tokio::test]
    async fn an_unreachable_launcher_fails_only_the_node_it_was_launching() {
        let service = test_service(unreachable_runner());

        let response = service.start(scan_request("10.0.0.5")).await;

        assert!(response.jobs.is_empty());
        let batch = response.response.expect("batch response");
        assert_eq!(batch.status, rm::ReturnCode::Failure as i32);
        assert_eq!(batch.node_results.len(), 1);
        assert_eq!(batch.node_results[0].status, rm::ReturnCode::Failure as i32);

        // The per-node job was still created (admission succeeded); only the
        // launch itself failed, and that failure seals the job.
        let child_job_id = service
            .job_tracker
            .get_job(&batch.job_id)
            .expect("parent job should exist")
            .child_job_ids
            .into_iter()
            .next()
            .expect("one child job should have been created");
        let child = service
            .job_tracker
            .get_job(&child_job_id)
            .expect("child job should exist");
        assert_eq!(child.state, JobState::Failed);
    }

    #[tokio::test]
    async fn a_launcher_that_rejects_the_scan_fails_the_job_it_created() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path_regex("^/runs$"))
            .respond_with(ResponseTemplate::new(500).set_body_string("nvssvt busy"))
            .mount(&server)
            .await;
        let service = test_service(runner_for(&server));

        let response = service.start(scan_request("10.0.0.5")).await;

        assert!(response.jobs.is_empty());
        let batch = response.response.expect("batch response");
        assert_eq!(batch.status, rm::ReturnCode::Failure as i32);
        assert_eq!(batch.node_results.len(), 1);
        assert!(
            batch.node_results[0].error_message.contains("nvssvt busy"),
            "{}",
            batch.node_results[0].error_message
        );
    }
}
