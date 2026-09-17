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

//! SPDM attestation evidence collection from NVIDIA NVOS switches.

use std::time::{Duration, Instant};

use nvue_client::action::{
    ActionStatus, STATE_ACTION_ERROR, STATE_ACTION_FAILED, STATE_ACTION_RUNNING,
    STATE_ACTION_SUCCESS, STATE_INACTIVE, STATE_START, endpoint as action_endpoint,
};
use nvue_client::system::{
    SPDM_OPERATIONAL_ENDPOINT, SpdmGenerateRequest, spdm_certificates_endpoint,
    spdm_component_endpoint, spdm_measurements_endpoint,
};
use serde_json::Value;

use super::{SwitchGb200Nvidia, extract_job_id, is_retryable_nvue_transport_error};
use crate::utilities::error::{Result, RmsError};

const SPDM_NONCE_SIZE_BYTES: usize = 32;
const MAX_SPDM_DIAGNOSTIC_CHARS: usize = 1024;
const SPDM_ACTION_TIMEOUT: Duration = Duration::from_secs(60);
const SPDM_ACTION_POLL_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug)]
enum SpdmActionCompletion {
    Pending,
    Succeeded,
    Failed(String),
}

/// Validated challenge supplied by the external attestation verifier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SpdmAttestationChallenge {
    pub(crate) nonce: Vec<u8>,
}

impl SpdmAttestationChallenge {
    /// Validate the SPDM nonce required by the NVUE generation action.
    pub(crate) fn new(nonce: Vec<u8>) -> Result<Self> {
        if nonce.len() != SPDM_NONCE_SIZE_BYTES {
            return Err(RmsError::invalid_argument(format!(
                "SPDM attestation nonce must contain exactly {SPDM_NONCE_SIZE_BYTES} bytes"
            )));
        }

        Ok(Self { nonce })
    }
}

/// Opaque signed measurements and certificate chain for one SPDM component.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SwitchSpdmAttestationEvidence {
    pub(crate) measurements: Vec<u8>,
    pub(crate) certificate_chain: Vec<u8>,
}

/// Collection result for one discovered SPDM component.
pub(crate) struct SwitchSpdmComponentResult {
    pub(crate) component_id: String,
    pub(crate) outcome: Result<SwitchSpdmAttestationEvidence>,
}

impl SwitchGb200Nvidia {
    /// Discover SPDM components and collect fresh evidence serially. A
    /// per-component failure (terminal or indeterminate) never aborts the
    /// rest of the switch's components: each is reported independently so
    /// the caller sees every component's status, not just the first failure.
    /// Deadline expiration preserves completed evidence and reports timeout
    /// failures for the current and remaining discovered components.
    pub(crate) async fn request_spdm_attestation_evidence(
        &self,
        challenge: &SpdmAttestationChallenge,
        deadline: Instant,
    ) -> Result<Vec<SwitchSpdmComponentResult>> {
        let nvue = self.nvue_client().map_err(bounded_error)?;

        let remaining = deadline.saturating_duration_since(Instant::now());

        if remaining.is_zero() {
            return Err(RmsError::timeout(
                "switch attestation batch exceeded its execution deadline",
            ));
        }

        let component_inventory = nvue
            .get_json(
                SPDM_OPERATIONAL_ENDPOINT,
                remaining.min(nvue_client::DEFAULT_TIMEOUT),
            )
            .await
            .map_err(RmsError::from)
            .map_err(bounded_error)?;

        let component_ids = parse_component_ids(&component_inventory).map_err(bounded_error)?;

        let mut results = Vec::with_capacity(component_ids.len());

        for component_id in component_ids {
            let remaining = deadline.saturating_duration_since(Instant::now());

            let outcome = if remaining.is_zero() {
                Err(RmsError::timeout(format!(
                    "switch attestation batch deadline elapsed before collecting component \
                     {component_id}"
                )))
            } else {
                match tokio::time::timeout(
                    remaining,
                    self.collect_spdm_component(&component_id, challenge),
                )
                .await
                {
                    Ok(outcome) => outcome,
                    Err(_) => Err(RmsError::timeout(format!(
                        "switch attestation batch deadline elapsed while collecting component \
                         {component_id}"
                    ))),
                }
            };

            results.push(SwitchSpdmComponentResult {
                component_id,
                outcome,
            });
        }

        Ok(results)
    }

    async fn collect_spdm_component(
        &self,
        component_id: &str,
        challenge: &SpdmAttestationChallenge,
    ) -> Result<SwitchSpdmAttestationEvidence> {
        let nvue = self.nvue_client().map_err(bounded_error)?;

        let request = SpdmGenerateRequest::new(hex::encode(&challenge.nonce));

        let response = nvue
            .post_json(
                &spdm_component_endpoint(component_id),
                &request,
                nvue_client::DEFAULT_TIMEOUT,
            )
            .await
            .map_err(RmsError::from)
            .map_err(bounded_error)?;

        let completion = self
            .wait_for_spdm_action(response, component_id, SPDM_ACTION_TIMEOUT)
            .await?;

        if let SpdmActionCompletion::Failed(message) = completion {
            return Err(RmsError::internal(message));
        }

        let measurements = nvue
            .get_json(
                &spdm_measurements_endpoint(component_id),
                nvue_client::DEFAULT_TIMEOUT,
            )
            .await
            .map_err(RmsError::from)
            .map_err(bounded_error)?;

        let certificates = nvue
            .get_json(
                &spdm_certificates_endpoint(component_id),
                nvue_client::DEFAULT_TIMEOUT,
            )
            .await
            .map_err(RmsError::from)
            .map_err(bounded_error)?;

        Ok(SwitchSpdmAttestationEvidence {
            measurements: serialize_evidence_value(measurements, "measurements", component_id)?,
            certificate_chain: serialize_evidence_value(
                certificates,
                "certificate chain",
                component_id,
            )?,
        })
    }

    async fn wait_for_spdm_action(
        &self,
        response: Value,
        component_id: &str,
        timeout: Duration,
    ) -> Result<SpdmActionCompletion> {
        let action_id = extract_job_id(&response).map_err(|error| {
            bounded_error(RmsError::internal(format!(
                "NVUE SPDM generation response for component {component_id} did not contain an \
                 action ID: {}",
                error.message
            )))
        })?;

        let nvue = self.nvue_client().map_err(bounded_error)?;
        let deadline = std::time::Instant::now() + timeout;

        while std::time::Instant::now() < deadline {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());

            let response = match nvue
                .get_json(
                    &action_endpoint(&action_id),
                    remaining.min(nvue_client::DEFAULT_TIMEOUT),
                )
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    let error = bounded_error(error.into());

                    if !is_retryable_nvue_transport_error(&error) {
                        return Err(error);
                    }

                    tracing::warn!(
                        action_id,
                        component_id,
                        error = %error.message,
                        "transient error polling NVUE SPDM action status; retrying"
                    );

                    sleep_until_next_poll(deadline).await;
                    continue;
                }
            };

            let completion =
                parse_spdm_action_status(response, component_id).map_err(bounded_error)?;

            match completion {
                SpdmActionCompletion::Pending => {}
                terminal => return Ok(terminal),
            }

            sleep_until_next_poll(deadline).await;
        }

        Err(bounded_error(RmsError::timeout(format!(
            "timed out waiting for NVUE SPDM generation action for component {component_id}"
        ))))
    }
}

async fn sleep_until_next_poll(deadline: std::time::Instant) {
    let remaining = deadline.saturating_duration_since(std::time::Instant::now());

    if !remaining.is_zero() {
        tokio::time::sleep(remaining.min(SPDM_ACTION_POLL_INTERVAL)).await;
    }
}

fn parse_spdm_action_status(response: Value, component_id: &str) -> Result<SpdmActionCompletion> {
    let status: ActionStatus = serde_json::from_value(response).map_err(|error| {
        RmsError::internal(format!(
            "invalid NVUE SPDM action response for component {component_id}: {error}"
        ))
    })?;

    let state = status.state();

    match state {
        STATE_ACTION_SUCCESS => Ok(SpdmActionCompletion::Succeeded),
        STATE_INACTIVE | STATE_START | "running" | STATE_ACTION_RUNNING => {
            Ok(SpdmActionCompletion::Pending)
        }
        STATE_ACTION_ERROR | STATE_ACTION_FAILED => {
            let issue = status.issue_message();

            let message = if !issue.is_empty() {
                issue
            } else if !status.detail_text().is_empty() {
                status.detail_text().to_owned()
            } else if !status.status_text().is_empty() {
                status.status_text().to_owned()
            } else {
                format!(
                    "NVUE SPDM generation for component {component_id} failed with state {state}"
                )
            };

            Ok(SpdmActionCompletion::Failed(bounded_diagnostic(&message)))
        }
        "" => Err(RmsError::internal(format!(
            "NVUE SPDM generation for component {component_id} returned no action state"
        ))),
        _ => Ok(SpdmActionCompletion::Failed(bounded_diagnostic(&format!(
            "NVUE SPDM generation for component {component_id} failed with unexpected state \
                 {state}"
        )))),
    }
}

fn parse_component_ids(inventory: &Value) -> Result<Vec<String>> {
    let components = inventory.as_object().ok_or_else(|| {
        RmsError::internal("NVUE SPDM operational response must be a component map")
    })?;

    let mut component_ids: Vec<_> = components
        .keys()
        .filter(|component_id| !component_id.trim().is_empty())
        .cloned()
        .collect();

    component_ids.sort();

    if component_ids.is_empty() {
        return Err(RmsError::failed_precondition(
            "NVUE SPDM operational response contains no components",
        ));
    }

    Ok(component_ids)
}

fn bounded_diagnostic(value: &str) -> String {
    value.chars().take(MAX_SPDM_DIAGNOSTIC_CHARS).collect()
}

fn bounded_error(error: RmsError) -> RmsError {
    RmsError::new(error.code, bounded_diagnostic(&error.message))
}

fn serialize_evidence_value(
    value: Value,
    evidence_type: &str,
    component_id: &str,
) -> Result<Vec<u8>> {
    if !matches!(&value, Value::Object(fields) if !fields.is_empty()) {
        return Err(RmsError::failed_precondition(format!(
            "NVUE SPDM {evidence_type} for component {component_id} must be a nonempty object"
        )));
    }

    serde_json::to_vec(&value).map_err(|error| {
        RmsError::internal(format!(
            "failed to serialize NVUE SPDM {evidence_type} for component {component_id}: {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn action_error_with_firmware_noop_text_remains_failure() {
        let completion = parse_spdm_action_status(
            serde_json::json!({
                "state": "action_error",
                "issue": [{"message": "Nothing to uninstall"}]
            }),
            "ERoT_BMC_0",
        )
        .unwrap();

        assert!(matches!(completion, SpdmActionCompletion::Failed(_)));
    }

    #[test]
    fn action_running_with_reboot_text_remains_pending() {
        let completion = parse_spdm_action_status(
            serde_json::json!({
                "state": "action_running",
                "status": "Switch is rebooting"
            }),
            "ERoT_BMC_0",
        )
        .unwrap();

        assert!(matches!(completion, SpdmActionCompletion::Pending));
    }

    #[test]
    fn only_literal_action_success_succeeds() {
        let completion =
            parse_spdm_action_status(serde_json::json!({"state": "action_success"}), "ERoT_BMC_0")
                .unwrap();

        assert!(matches!(completion, SpdmActionCompletion::Succeeded));

        for state in ["success", "ACTION_SUCCESS", "action_cancelled"] {
            let completion =
                parse_spdm_action_status(serde_json::json!({"state": state}), "ERoT_BMC_0")
                    .unwrap();

            assert!(matches!(completion, SpdmActionCompletion::Failed(_)));
        }

        assert!(parse_spdm_action_status(serde_json::json!({}), "ERoT_BMC_0").is_err());
    }

    #[test]
    fn evidence_must_be_a_nonempty_object() {
        for value in [
            Value::Null,
            serde_json::json!({}),
            serde_json::json!([]),
            serde_json::json!(""),
        ] {
            assert!(serialize_evidence_value(value, "measurements", "ERoT_BMC_0").is_err());
        }

        assert!(
            serialize_evidence_value(
                serde_json::json!({"signed_measurements": "value"}),
                "measurements",
                "ERoT_BMC_0",
            )
            .is_ok()
        );
    }

    #[tokio::test]
    async fn action_poll_timeout_reports_a_timeout_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/action-timeout"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "state": "start"
            })))
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        let error = switch
            .wait_for_spdm_action(
                serde_json::json!("action-timeout"),
                "ERoT_BMC_0",
                Duration::from_millis(25),
            )
            .await
            .unwrap_err();

        assert_eq!(error.code, crate::utilities::error::ErrorCode::Timeout);
        assert!(error.message.contains("ERoT_BMC_0"));
    }

    #[tokio::test]
    async fn action_poll_does_not_retry_malformed_status() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/action-malformed"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        let error = switch
            .wait_for_spdm_action(
                serde_json::json!("action-malformed"),
                "ERoT_BMC_0",
                Duration::from_secs(5),
            )
            .await
            .unwrap_err();

        assert_eq!(error.code, crate::utilities::error::ErrorCode::Internal);
        assert!(error.message.contains("returned no action state"));
    }

    #[tokio::test]
    async fn action_poll_does_not_retry_permanent_http_errors() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/action-unauthorized"))
            .respond_with(ResponseTemplate::new(401))
            .expect(1)
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        let error = switch
            .wait_for_spdm_action(
                serde_json::json!("action-unauthorized"),
                "ERoT_BMC_0",
                Duration::from_secs(2),
            )
            .await
            .unwrap_err();

        assert_eq!(
            error.code,
            crate::utilities::error::ErrorCode::Unauthenticated
        );
    }

    #[tokio::test]
    async fn collection_deadline_preserves_completed_and_unattempted_component_results() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/system/security/spdm"))
            .and(wiremock::matchers::query_param("rev", "operational"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ERoT_BMC_0": {},
                "ERoT_CPU_0": {},
                "ERoT_FPGA_0": {}
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/nvue_v1/system/security/spdm/ERoT_BMC_0"))
            .respond_with(ResponseTemplate::new(201).set_body_json("spdm-bmc"))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/spdm-bmc"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "state": "action_success"
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(
                "/nvue_v1/system/security/spdm/ERoT_BMC_0/measurements",
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"signed": true})),
            )
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(
                "/nvue_v1/system/security/spdm/ERoT_BMC_0/certificates",
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"chain": true})),
            )
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/nvue_v1/system/security/spdm/ERoT_CPU_0"))
            .respond_with(
                ResponseTemplate::new(201)
                    .set_delay(Duration::from_secs(10))
                    .set_body_json("spdm-cpu"),
            )
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/nvue_v1/system/security/spdm/ERoT_FPGA_0"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());
        let challenge = SpdmAttestationChallenge::new(vec![0xcd; 32]).unwrap();

        let results = switch
            .request_spdm_attestation_evidence(
                &challenge,
                std::time::Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].component_id, "ERoT_BMC_0");
        assert!(results[0].outcome.is_ok());
        assert_eq!(results[1].component_id, "ERoT_CPU_0");
        assert_eq!(results[2].component_id, "ERoT_FPGA_0");

        for result in &results[1..] {
            assert_eq!(
                result.outcome.as_ref().unwrap_err().code,
                crate::utilities::error::ErrorCode::Timeout
            );
        }

        server.verify().await;
    }

    /// Regression test for a fixed bug: an indeterminate component outcome
    /// (here, an action-poll timeout) must not discard the evidence already
    /// collected for the switch's other components.
    #[tokio::test]
    async fn indeterminate_component_failure_does_not_abort_remaining_components() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/system/security/spdm"))
            .and(wiremock::matchers::query_param("rev", "operational"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "ERoT_BMC_0": {},
                "ERoT_CPU_0": {}
            })))
            .mount(&server)
            .await;

        // A generation response with no action ID is an indeterminate outcome
        // (the request may or may not have started a device-side action):
        // it fails immediately without a poll delay, keeping this test fast.
        Mock::given(method("POST"))
            .and(path("/nvue_v1/system/security/spdm/ERoT_BMC_0"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/nvue_v1/system/security/spdm/ERoT_CPU_0"))
            .respond_with(ResponseTemplate::new(201).set_body_json("spdm-cpu"))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/spdm-cpu"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "state": "action_success"
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(
                "/nvue_v1/system/security/spdm/ERoT_CPU_0/measurements",
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"signed": true})),
            )
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(
                "/nvue_v1/system/security/spdm/ERoT_CPU_0/certificates",
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"chain": true})),
            )
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());
        let challenge = SpdmAttestationChallenge::new(vec![0xcd; 32]).unwrap();

        let results = switch
            .request_spdm_attestation_evidence(&challenge, Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].component_id, "ERoT_BMC_0");
        assert!(results[0].outcome.is_err());
        assert_eq!(results[1].component_id, "ERoT_CPU_0");
        assert!(results[1].outcome.is_ok());
    }
}
