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

//! Synchronous switch SPDM attestation evidence collection.

use std::collections::HashSet;
use std::net::IpAddr;
use std::time::{Duration, Instant};

use chrono::Utc;
use futures::future::join_all;
use librms::protos::rack_manager as rm;

use super::server::RackManagerServiceImpl;
use crate::api::grpc::conversions::{
    flatten_node_info, proto_node_type_to_domain, timestamp_from_datetime,
};
use crate::domain::node::NodeKind;
use crate::domain::rack::NodeConfig;
use crate::nodes::NodeInstance;
use crate::nodes::switch_gb200_nvidia::{
    SpdmAttestationChallenge, SwitchGb200Nvidia, SwitchSpdmComponentResult,
};
use crate::utilities::error::{Result, RmsError};

const ATTESTATION_BATCH_TIMEOUT: Duration = Duration::from_secs(15 * 60);

fn prepare_attestation_target(device: &rm::NodeInfo) -> Result<Box<SwitchGb200Nvidia>> {
    let node_type_raw = device.r#type.unwrap_or(rm::NodeType::Unspecified as i32);

    let node_type = proto_node_type_to_domain(node_type_raw)
        .ok_or_else(|| RmsError::invalid_argument(format!("unknown node type {node_type_raw}")))?;

    if node_type.kind() != NodeKind::Switch {
        return Err(RmsError::invalid_argument(format!(
            "node type {node_type} is not a switch"
        )));
    }

    let flat = flatten_node_info(device)?;

    if flat.creds_for_node_type(node_type).is_none() {
        return Err(RmsError::invalid_argument(
            "switch host credentials are required for SPDM attestation",
        ));
    }

    let host_endpoint = flat.switch_host_management_endpoint()?;

    let config = NodeConfig {
        id: device.node_id.clone(),
        node_type,
        bmc_endpoint: None,
        host_endpoint: Some(host_endpoint),
        expected_inventory: None,
    };

    NodeInstance::create_switch_password(&config, &device.rack_id)
}

fn physical_switch_key(device: &rm::NodeInfo) -> Option<String> {
    let address = device
        .host_endpoint
        .as_ref()?
        .interface
        .as_ref()?
        .ip_address
        .trim();

    canonical_switch_key(address)
}

fn canonical_switch_key(address: &str) -> Option<String> {
    let address = address.trim();

    let address = address
        .strip_prefix('[')
        .and_then(|address| address.strip_suffix(']'))
        .unwrap_or(address);

    (!address.is_empty()).then(|| {
        address
            .parse::<IpAddr>()
            .map_or_else(|_| address.to_ascii_lowercase(), |ip| ip.to_string())
    })
}

fn validate_attestation_targets(
    devices: &[rm::NodeInfo],
) -> std::result::Result<(), tonic::Status> {
    let mut seen_logical_targets = HashSet::with_capacity(devices.len());
    let mut seen_physical_targets = HashSet::with_capacity(devices.len());

    for device in devices {
        let node_id = device.node_id.trim();

        if node_id.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "switch attestation target node_id must not be empty",
            ));
        }

        if !seen_logical_targets.insert(node_id) {
            return Err(tonic::Status::invalid_argument(format!(
                "duplicate switch target in request: node_id={}",
                device.node_id
            )));
        }

        if let Some(physical_key) = physical_switch_key(device)
            && !seen_physical_targets.insert(physical_key.clone())
        {
            return Err(tonic::Status::invalid_argument(format!(
                "duplicate physical switch target in request: host_ip={physical_key}"
            )));
        }
    }

    Ok(())
}

impl RackManagerServiceImpl {
    pub(crate) async fn handle_batch_collect_switch_spdm_attestation_evidence(
        &self,
        req: tonic::Request<rm::BatchCollectSwitchSpdmAttestationEvidenceRequest>,
    ) -> std::result::Result<
        tonic::Response<rm::BatchCollectSwitchSpdmAttestationEvidenceResponse>,
        tonic::Status,
    > {
        let request = req.into_inner();
        let deadline = Instant::now() + ATTESTATION_BATCH_TIMEOUT;

        let challenge = SpdmAttestationChallenge::new(request.nonce)
            .map_err(|error| tonic::Status::invalid_argument(error.message))?;

        if request.targets.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "targets is required and must contain at least one switch",
            ));
        }

        validate_attestation_targets(&request.targets)?;

        // Targets are collected concurrently against one shared deadline.
        // Collection remains directly owned by the RPC so cancellation drops
        // in-flight NVUE work.
        let results = join_all(request.targets.into_iter().map(|device| {
            let node_id = device.node_id.clone();
            let rack_id = device.rack_id.clone();
            let service = self.clone();
            let challenge = challenge.clone();
            let domain = request.domain.clone();

            async move {
                let collected = async {
                    let switch = prepare_attestation_target(&device)?;
                    let remaining = deadline.saturating_duration_since(Instant::now());

                    if remaining.is_zero() {
                        return Err(RmsError::timeout(
                            "switch attestation batch exceeded its execution deadline",
                        ));
                    }

                    match tokio::time::timeout(
                        remaining,
                        service.initialize_nvue_client(
                            switch.optional_nvue_client(),
                            domain.as_deref(),
                        ),
                    )
                    .await
                    {
                        Ok(result) => result?,
                        Err(_) => {
                            return Err(RmsError::timeout(
                                "switch attestation batch exceeded its execution deadline",
                            ));
                        }
                    }

                    switch
                        .request_spdm_attestation_evidence(&challenge, deadline)
                        .await
                }
                .await;

                match collected {
                    Ok(components) => target_result_from_components(node_id, components),
                    Err(error) => {
                        tracing::warn!(
                            node = %node_id,
                            rack = %rack_id,
                            error = %error.message,
                            "switch SPDM attestation evidence collection failed"
                        );

                        rm::SwitchSpdmAttestationTargetResult {
                            node_id,
                            status: rm::ReturnCode::Failure.into(),
                            error_message: Some(error.message),
                            components: Vec::new(),
                            collection_timestamp: Some(timestamp_from_datetime(Utc::now())),
                        }
                    }
                }
            }
        }))
        .await;

        let mut response = rm::BatchCollectSwitchSpdmAttestationEvidenceResponse {
            results,
            ..Default::default()
        };

        update_batch_summary(&mut response);

        Ok(tonic::Response::new(response))
    }
}

fn target_result_from_components(
    node_id: String,
    components: Vec<SwitchSpdmComponentResult>,
) -> rm::SwitchSpdmAttestationTargetResult {
    let mut component_results = Vec::with_capacity(components.len());

    for component in components {
        let component_id = component.component_id;

        let result = match component.outcome {
            Ok(evidence) => rm::SwitchSpdmComponentResult {
                component_id,
                status: rm::ReturnCode::Success.into(),
                error_message: String::new(),
                evidence: Some(rm::SwitchSpdmComponentEvidence {
                    measurements: evidence.measurements,
                    certificate_chain: evidence.certificate_chain,
                }),
            },
            Err(error) => {
                tracing::warn!(
                    node = %node_id,
                    component_id,
                    error = %error.message,
                    "switch SPDM component evidence collection failed"
                );

                rm::SwitchSpdmComponentResult {
                    component_id,
                    status: rm::ReturnCode::Failure.into(),
                    error_message: error.message,
                    evidence: None,
                }
            }
        };

        component_results.push(result);
    }

    let all_succeeded = component_results
        .iter()
        .all(|component| component.status == rm::ReturnCode::Success as i32);

    rm::SwitchSpdmAttestationTargetResult {
        node_id,
        status: if all_succeeded {
            rm::ReturnCode::Success
        } else {
            rm::ReturnCode::Failure
        }
        .into(),
        error_message: None,
        components: component_results,
        collection_timestamp: Some(timestamp_from_datetime(Utc::now())),
    }
}

fn update_batch_summary(response: &mut rm::BatchCollectSwitchSpdmAttestationEvidenceResponse) {
    let successful_targets = response
        .results
        .iter()
        .filter(|target| target.status == rm::ReturnCode::Success as i32)
        .count();

    let total_targets = response.results.len();

    response.status = if successful_targets == total_targets {
        rm::ReturnCode::Success
    } else {
        rm::ReturnCode::Failure
    }
    .into();

    response.message = format!(
        "SPDM attestation evidence available for {successful_targets} of {total_targets} switches"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::switch_gb200_nvidia::SwitchSpdmAttestationEvidence;

    fn successful_component(component_id: &str) -> SwitchSpdmComponentResult {
        SwitchSpdmComponentResult {
            component_id: component_id.to_owned(),
            outcome: Ok(SwitchSpdmAttestationEvidence {
                measurements: br#"{"measurement":"signed"}"#.to_vec(),
                certificate_chain: br#"{"certificate":"chain"}"#.to_vec(),
            }),
        }
    }

    #[test]
    fn target_mapping_preserves_successful_evidence() {
        let target = target_result_from_components(
            "switch-1".to_owned(),
            vec![successful_component("ERoT_BMC_0")],
        );

        assert_eq!(target.status, rm::ReturnCode::Success as i32);
        assert_eq!(target.components.len(), 1);
        assert_eq!(target.components[0].status, rm::ReturnCode::Success as i32);
        assert!(target.components[0].evidence.is_some());
    }

    #[test]
    fn target_and_batch_mapping_report_partial_failure() {
        let target = target_result_from_components(
            "switch-1".to_owned(),
            vec![
                successful_component("ERoT_BMC_0"),
                SwitchSpdmComponentResult {
                    component_id: "ERoT_CPU_0".to_owned(),
                    outcome: Err(RmsError::internal("generation failed")),
                },
            ],
        );

        let mut batch = rm::BatchCollectSwitchSpdmAttestationEvidenceResponse {
            results: vec![target],
            ..Default::default()
        };

        update_batch_summary(&mut batch);

        assert_eq!(batch.status, rm::ReturnCode::Failure as i32);
        assert_eq!(batch.results[0].status, rm::ReturnCode::Failure as i32);
        assert!(batch.results[0].error_message.is_none());

        assert_eq!(
            batch.results[0].components[1].status,
            rm::ReturnCode::Failure as i32
        );

        assert!(batch.results[0].components[1].evidence.is_none());
    }

    #[test]
    fn canonical_switch_key_normalizes_bracketed_ipv6() {
        assert_eq!(
            canonical_switch_key("[2001:0db8::1]"),
            canonical_switch_key("2001:db8::1")
        );
    }
}
