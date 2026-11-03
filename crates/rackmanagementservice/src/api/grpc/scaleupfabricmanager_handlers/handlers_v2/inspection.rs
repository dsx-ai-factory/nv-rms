/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Reads live fabric status without starting jobs or using layout configuration.
//!
//! All reads share one deadline. Results from completed reads are kept when other
//! reads fail. Successful collection does not mean the hardware is healthy, and
//! the reads can reflect different points in time.

mod collection;
mod membership;
mod projection;
mod readback;

use std::collections::HashMap;
use std::time::Duration;

use self::readback::{apply_samples, controller_observation};
use super::EphemeralSwitch;
use super::observations::sample_group;
use super::observations::{FabricSamples, ObservedIdentity};
use crate::api::grpc::node_type_resolver::normalize_node_set;
use crate::api::grpc::server::RackManagerServiceImpl;
use crate::domain::node::NodeKind;

use librms::protos::rack_manager as rm;

use futures::future::join_all;
use tokio::time::{Instant, timeout_at};

const INSPECTION_TIMEOUT: Duration = Duration::from_secs(60);

/// Tracks whether a resource was read and any errors found while interpreting it.
#[derive(Default)]
struct ReadEvidence {
    collected: bool,
    errors: Vec<String>,
}

#[derive(Default)]
struct SwitchEvidence {
    node_id: String,
    enabled: Option<bool>,
    location: Option<ObservedIdentity>,
    reads: HashMap<String, ReadEvidence>,
}

#[derive(Default)]
struct ComputeEvidence {
    node_id: String,
    location: Option<ObservedIdentity>,
    reads: HashMap<String, ReadEvidence>,
}

#[derive(Default)]
struct FabricEvidence {
    observation: rm::ScaleUpFabricInspectionObservation,
    reads: HashMap<String, ReadEvidence>,
}

impl RackManagerServiceImpl {
    /// Reads live fabric status without starting jobs or changing devices.
    ///
    /// Fabrics follow switch inventory order; node IDs within each fabric are sorted.
    /// Invalid inventory returns before device reads. Read or interpretation errors
    /// return FAILURE with available results; abnormal hardware alone does not.
    pub(crate) async fn handle_inspect_scale_up_fabrics(
        &self,
        request: tonic::Request<rm::InspectScaleUpFabricsRequest>,
    ) -> Result<tonic::Response<rm::InspectScaleUpFabricsResponse>, tonic::Status> {
        let deadline = inspection_deadline(request.metadata())?;

        let mut request = request.into_inner();
        let mut observation = rm::InspectScaleUpFabricsResponse::default();

        let validation = normalize_node_set(&mut request.nodes)
            .map_err(|error| error.to_string())
            .and_then(|()| validate_inspection(&request));

        let (switches, computes) = match validation {
            Ok(inventory) => inventory,
            Err(message) => {
                observation
                    .discovery_errors
                    .push(rm::ScaleUpFabricInspectionError {
                        node_id: String::new(),
                        message: message.clone(),
                    });

                observation.status = rm::ReturnCode::Failure.into();
                observation.error_message = message;

                return Ok(tonic::Response::new(observation));
            }
        };

        // Read switches and computes concurrently. Dropping the RPC cancels these reads.
        let switch_reads = switches
            .iter()
            .map(|node| self.inspect_switch(node, request.domain.as_deref(), deadline));

        let compute_reads = computes
            .iter()
            .map(|node| self.inspect_compute(node, deadline));

        let (switch_reads, compute_reads) =
            tokio::join!(join_all(switch_reads), join_all(compute_reads),);

        // Wait for identity collection before matching controller-reported devices
        // to caller node IDs. Controller reads have already run alongside discovery.
        let fabric_reads = switch_reads
            .iter()
            .filter(|(switch, _)| switch.enabled == Some(true))
            .map(|(primary, samples)| {
                let mut observed = controller_observation(&primary.node_id);

                if let Some(samples) = samples {
                    apply_samples(
                        &mut observed,
                        samples,
                        switch_reads.iter().map(|(switch, _)| switch),
                        &compute_reads,
                    );
                }

                observed
            })
            .collect();

        projection::project_observation(
            &mut observation,
            fabric_reads,
            switch_reads
                .into_iter()
                .map(|(observed, _)| observed)
                .collect(),
            compute_reads,
        );

        let incomplete = !observation.discovery_errors.is_empty()
            || observation
                .fabrics
                .iter()
                .any(|fabric| !fabric.inspection_errors.is_empty());

        observation.status = if incomplete {
            rm::ReturnCode::Failure
        } else {
            rm::ReturnCode::Success
        }
        .into();

        if incomplete {
            observation.error_message =
                "one or more observations are incomplete or ambiguous".into();
        }

        Ok(tonic::Response::new(observation))
    }

    /// Reads an enabled controller without waiting for physical identities.
    /// Records connection failures against the primary switch.
    async fn inspect_controller(
        &self,
        prepared: &EphemeralSwitch,
        domain: Option<&str>,
        deadline: Instant,
        reads: &mut HashMap<String, ReadEvidence>,
    ) -> Option<FabricSamples> {
        let mut client = collect_read(
            timeout_at(
                deadline,
                self.connect_nmx_controller(
                    &prepared.target_ip.to_string(),
                    &prepared.tls_server_name,
                    domain,
                    false,
                ),
            )
            .await,
            "controller-connection",
            reads,
        )?;

        Some(sample_group(client.as_mut(), &self.nmx_gateway_id, Some(deadline)).await)
    }
}

/// Caps collection at 60 seconds and reserves time to return results before a caller timeout.
/// Invalid timeout metadata is rejected before collection.
pub(super) fn inspection_deadline(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<Instant, tonic::Status> {
    let mut budget = INSPECTION_TIMEOUT;

    if let Some(value) = metadata.get("grpc-timeout") {
        let value = value
            .to_str()
            .map_err(|_| tonic::Status::invalid_argument("invalid grpc-timeout"))?;

        if !value.is_ascii() {
            return Err(tonic::Status::invalid_argument("invalid grpc-timeout"));
        }

        let (digits, unit) = value.split_at(value.len().saturating_sub(1));

        let amount = digits
            .parse::<u64>()
            .ok()
            .filter(|_| {
                !digits.is_empty()
                    && digits.len() <= 8
                    && digits.bytes().all(|byte| byte.is_ascii_digit())
            })
            .ok_or_else(|| tonic::Status::invalid_argument("invalid grpc-timeout"))?;

        let duration = match unit {
            "H" => Duration::from_secs(amount * 3600),
            "M" => Duration::from_secs(amount * 60),
            "S" => Duration::from_secs(amount),
            "m" => Duration::from_millis(amount),
            "u" => Duration::from_micros(amount),
            "n" => Duration::from_nanos(amount),
            _ => return Err(tonic::Status::invalid_argument("invalid grpc-timeout")),
        };

        // Tonic starts the transport timer before the handler. Leave time for
        // response construction and delivery; very short timeouts allow no reads.
        let response_time = (duration / 10).max(Duration::from_millis(250));

        budget = budget.min(duration.saturating_sub(response_time));
    }

    if budget.is_zero() {
        return Err(tonic::Status::deadline_exceeded(
            "grpc-timeout leaves no time for inspection",
        ));
    }

    Ok(Instant::now() + budget)
}

/// Checks node IDs and types, then separates switches from computes.
fn validate_inspection(
    request: &rm::InspectScaleUpFabricsRequest,
) -> Result<(Vec<&rm::NodeInfo>, Vec<&rm::NodeInfo>), String> {
    let nodes = request
        .nodes
        .as_ref()
        .map(|nodes| nodes.nodes.as_slice())
        .unwrap_or_default();

    let types = super::inventory::validate(nodes).map_err(|error| error.message().to_owned())?;

    let mut switches = Vec::new();
    let mut computes = Vec::new();

    for (node, node_type) in nodes.iter().zip(types) {
        match node_type.kind() {
            NodeKind::Switch => switches.push(node),
            NodeKind::Compute => computes.push(node),
            NodeKind::Powershelf => {
                return Err("inspection accepts only switch and compute nodes".into());
            }
        }
    }

    if switches.is_empty() {
        return Err("inspection requires at least one switch".into());
    }

    Ok((switches, computes))
}

/// Adds an error without changing whether the resource was read successfully.
fn record_read_error(reads: &mut HashMap<String, ReadEvidence>, resource: &str, message: &str) {
    let result = reads.entry(resource.into()).or_default();

    result.errors.push(message.into());
}

/// Records success, failure, or timeout without exposing raw transport errors.
/// A successful read may still contain data that cannot be interpreted.
fn collect_read<T, E>(
    result: Result<Result<T, E>, tokio::time::error::Elapsed>,
    resource: &str,
    reads: &mut HashMap<String, ReadEvidence>,
) -> Option<T> {
    let (value, error) = match result {
        Ok(Ok(value)) => (Some(value), None),
        Ok(Err(_)) => (None, Some("resource read failed")),
        Err(_) => (None, Some("collection deadline exceeded")),
    };

    reads.entry(resource.into()).or_default().collected = value.is_some();

    if let Some(error) = error {
        record_read_error(reads, resource, error);
    }

    value
}

#[cfg(test)]
mod tests;
