/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Reads switch state and physical identities without changing devices.

use super::super::build_ephemeral_switch;
use super::{
    ComputeEvidence, FabricSamples, ObservedIdentity, ReadEvidence, SwitchEvidence, collect_read,
    record_read_error,
};
use crate::api::grpc::server::RackManagerServiceImpl;
use crate::utilities::error::ErrorCode;

use librms::protos::rack_manager as rm;
use nvue_client::DEFAULT_TIMEOUT;

use serde::Deserialize;
use tokio::time::{Instant, timeout_at};

#[derive(Deserialize)]
struct ComputeIdentity {
    chassis_sn: String,
    tray_index: u32,
}

impl RackManagerServiceImpl {
    /// Starts controller reads as soon as the switch reports an enabled cluster.
    /// Reads identity and controller status concurrently under the same deadline.
    /// Reports an unknown cluster state as an error rather than treating it as disabled.
    pub(super) async fn inspect_switch(
        &self,
        node: &rm::NodeInfo,
        domain: Option<&str>,
        deadline: Instant,
    ) -> (SwitchEvidence, Option<FabricSamples>) {
        let mut observed = SwitchEvidence {
            node_id: node.node_id.clone(),
            reads: ["cluster", "identity"]
                .into_iter()
                .map(|resource| {
                    (
                        resource.to_owned(),
                        ReadEvidence {
                            collected: false,
                            ..Default::default()
                        },
                    )
                })
                .collect(),
            ..Default::default()
        };

        // Keep local input errors actionable, but redact failures from transport setup.
        let prepared = match build_ephemeral_switch(node, self.insecure_bmc) {
            Ok(prepared) => prepared,
            Err(error) => {
                let message = if matches!(
                    error.code,
                    ErrorCode::InvalidArgument | ErrorCode::FailedPrecondition
                ) {
                    error.message.as_str()
                } else {
                    "resource read failed"
                };

                record_read_error(&mut observed.reads, "switch-connection", message);

                return (observed, None);
            }
        };

        let initialized = collect_read(
            timeout_at(
                deadline,
                self.initialize_nvue_client_with_options(
                    prepared.switch.nvue_client(),
                    domain,
                    Some(&prepared.tls_server_name),
                    false,
                ),
            )
            .await,
            "switch-connection",
            &mut observed.reads,
        );

        if initialized.is_none() {
            return (observed, None);
        }

        let Some(nvue) = prepared.switch.nvue_client() else {
            record_read_error(
                &mut observed.reads,
                "switch-connection",
                "NVUE client is unavailable",
            );

            return (observed, None);
        };

        let controller = async {
            let cluster = timeout_at(deadline, nvue.get_cluster(DEFAULT_TIMEOUT)).await;

            if let Some(cluster) = collect_read(cluster, "cluster", &mut observed.reads) {
                observed.enabled = match cluster.state.as_deref() {
                    Some("enabled" | "start") => Some(true),
                    Some("disabled") => Some(false),
                    _ => None,
                };

                if observed.enabled.is_none() {
                    record_read_error(
                        &mut observed.reads,
                        "cluster",
                        "cluster state is unrecognized",
                    );
                }
            }

            if observed.enabled == Some(true) {
                self.inspect_controller(&prepared, domain, deadline, &mut observed.reads)
                    .await
            } else {
                None
            }
        };

        let (samples, identity) = tokio::join!(
            controller,
            timeout_at(deadline, nvue.get_chassis_location(DEFAULT_TIMEOUT))
        );

        if let Some(identity) = collect_read(identity, "identity", &mut observed.reads) {
            observed.location = identity
                .chassis_sn
                .filter(|serial| !serial.trim().is_empty())
                .zip(
                    identity
                        .tray_index
                        .as_deref()
                        .and_then(|value| value.parse().ok()),
                )
                .map(|(serial, tray_index)| ObservedIdentity {
                    node_id: node.node_id.clone(),
                    chassis_serial_number: serial,
                    tray_index,
                });

            if observed.location.is_none() {
                record_read_error(
                    &mut observed.reads,
                    "identity",
                    "physical identity is incomplete",
                );

                observed
                    .reads
                    .entry("identity".into())
                    .or_default()
                    .collected = false;
            }
        }

        (observed, samples)
    }

    /// Reads physical identity through the compute BMC; no host login is needed.
    /// Reports missing chassis or tray identity as an error, even if the read succeeds.
    pub(super) async fn inspect_compute(
        &self,
        info: &rm::NodeInfo,
        deadline: Instant,
    ) -> ComputeEvidence {
        let mut observed = ComputeEvidence {
            node_id: info.node_id.clone(),
            reads: [(
                "identity".into(),
                ReadEvidence {
                    collected: false,
                    ..Default::default()
                },
            )]
            .into(),
            ..Default::default()
        };

        let node = match self.build_batch_ephemeral_node(info) {
            Ok(Some(node)) => node,
            Ok(None) => {
                record_read_error(
                    &mut observed.reads,
                    "identity",
                    "compute discovery adapter is unavailable",
                );

                return observed;
            }
            Err(message) => {
                record_read_error(&mut observed.reads, "identity", &message);

                return observed;
            }
        };

        let payload = collect_read(
            timeout_at(deadline, node.get_device_info()).await,
            "identity",
            &mut observed.reads,
        )
        .flatten();

        let identity =
            payload.and_then(|payload| serde_json::from_value::<ComputeIdentity>(payload).ok());

        let Some(identity) = identity.filter(|identity| !identity.chassis_sn.trim().is_empty())
        else {
            observed
                .reads
                .entry("identity".into())
                .or_default()
                .collected = false;

            if observed.reads.values().all(|read| read.errors.is_empty()) {
                record_read_error(
                    &mut observed.reads,
                    "identity",
                    "physical identity is incomplete",
                );
            }

            return observed;
        };

        observed.location = Some(ObservedIdentity {
            node_id: info.node_id.clone(),
            chassis_serial_number: identity.chassis_sn,
            tray_index: identity.tray_index,
        });

        observed
            .reads
            .entry("identity".into())
            .or_default()
            .collected = true;

        observed
    }
}
