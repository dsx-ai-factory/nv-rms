/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
 */

//! Handlers for switch security RPCs.
//!
//! `UpdateSwitchSystemPassword` is a synchronous per-switch workflow:
//! create revision -> patch user password -> apply -> save.
//! Multi-node requests run those per-switch workflows in parallel and return a
//! synchronous batch response after every requested device completes.

use std::collections::HashSet;

use super::server::RackManagerServiceImpl;
use crate::api::grpc::conversions::{flatten_node_info, proto_node_type_to_domain};
use crate::domain::node::NodeKind;
use crate::domain::rack::NodeConfig;
use crate::nodes::NodeInstance;
use librms::protos::rack_manager as rm;

use futures::future::join_all;

fn redact_password_rotation_error(message: &str) -> String {
    nvfwupd::utils::Util::redact_secret_fields(message)
}

fn validate_unique_switch_password_targets(devices: &[rm::NodeInfo]) -> Result<(), tonic::Status> {
    let mut seen_targets = HashSet::with_capacity(devices.len());

    for device in devices {
        let target = (device.rack_id.as_str(), device.node_id.as_str());

        if !seen_targets.insert(target) {
            return Err(tonic::Status::invalid_argument(format!(
                "duplicate switch target in request: rack_id={} node_id={}",
                device.rack_id, device.node_id
            )));
        }
    }

    Ok(())
}

impl RackManagerServiceImpl {
    pub(crate) async fn handle_update_switch_system_password(
        &self,
        req: tonic::Request<rm::UpdateSwitchSystemPasswordRequest>,
    ) -> std::result::Result<tonic::Response<rm::UpdateSwitchSystemPasswordResponse>, tonic::Status>
    {
        let r = req.into_inner();
        if r.username.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "username is required and must be non-empty",
            ));
        }
        if !r.username.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(tonic::Status::invalid_argument(
                "username may only contain ASCII letters and digits",
            ));
        }
        if r.password.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "password is required and must be non-empty",
            ));
        }

        let devices = r.nodes.map(|ns| ns.nodes).unwrap_or_default();
        if devices.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "nodes is required and must contain at least one device",
            ));
        }

        let total_nodes = devices.len() as u32;
        let username = r.username;
        let password = r.password;

        validate_unique_switch_password_targets(&devices)?;

        tracing::info!(
            target_user = %username,
            device_count = total_nodes,
            "update switch system password request received"
        );

        // The RPC stays synchronous: every requested switch finishes before
        // returning. We do not add a service-side concurrency limit here; the
        // caller owns batch sizing, and each per-node failure is captured in
        // `node_results`.
        let node_results =
            join_all(devices.into_iter().map(|device| {
                self.update_switch_password_for_device(device, &username, &password)
            }))
            .await;

        debug_assert_eq!(node_results.len(), total_nodes as usize);

        let successful_nodes = node_results
            .iter()
            .filter(|result| result.status == rm::ReturnCode::Success as i32)
            .count() as u32;

        let failed_nodes = total_nodes.saturating_sub(successful_nodes);

        // Preserve the existing API contract: any node failure makes the batch
        // fail, while successful nodes and failure reasons are still reported
        // individually.
        let batch_status = if failed_nodes == 0 {
            rm::ReturnCode::Success
        } else {
            rm::ReturnCode::Failure
        };

        tracing::info!(
            target_user = %username,
            successful_nodes,
            failed_nodes,
            total_nodes,
            "completed switch password rotation batch"
        );

        Ok(tonic::Response::new(
            rm::UpdateSwitchSystemPasswordResponse {
                response: Some(rm::NodeBatchResponse {
                    status: batch_status.into(),
                    message: format!(
                        "Updated switch system password on {} of {} devices",
                        successful_nodes, total_nodes
                    ),
                    node_results,
                    job_id: String::new(),
                    stats: Some(rm::NodeOperationStats {
                        total_nodes,
                        successful_nodes,
                        failed_nodes,
                    }),
                }),
            },
        ))
    }

    async fn update_switch_password_for_device(
        &self,
        device: rm::NodeInfo,
        username: &str,
        password: &str,
    ) -> rm::NodeOperationResult {
        let node_id = device.node_id.clone();
        let rack_id = device.rack_id.clone();

        tracing::info!(
            node = %node_id,
            rack = %rack_id,
            target_user = %username,
            "processing switch password rotation request"
        );

        let node_type_raw = device.r#type.unwrap_or(rm::NodeType::Unspecified as i32);

        let Some(node_type) = proto_node_type_to_domain(node_type_raw) else {
            tracing::warn!(
                node = %node_id,
                rack = %rack_id,
                target_user = %username,
                node_type = node_type_raw,
                "skipping password rotation for unknown node type"
            );

            let error_message = format!("device {node_id} is not a switch (type={node_type_raw})");

            return rm::NodeOperationResult {
                node_id,
                status: rm::ReturnCode::Failure.into(),
                error_message,
            };
        };

        if node_type.kind() != NodeKind::Switch {
            tracing::warn!(
                node = %node_id,
                rack = %rack_id,
                target_user = %username,
                node_type = %node_type,
                "skipping password rotation for non-switch node"
            );

            let error_message = format!("device {node_id} is not a switch (type={node_type})");

            return rm::NodeOperationResult {
                node_id,
                status: rm::ReturnCode::Failure.into(),
                error_message,
            };
        }

        let flat = match flatten_node_info(&device) {
            Ok(flat) => flat,
            Err(e) => {
                let message = e.message;

                tracing::warn!(
                    node = %node_id,
                    rack = %rack_id,
                    target_user = %username,
                    error = %message,
                    "skipping password rotation due to invalid endpoint credentials"
                );

                return rm::NodeOperationResult {
                    node_id,
                    status: rm::ReturnCode::Failure.into(),
                    error_message: message,
                };
            }
        };

        if flat.creds_for_node_type(node_type).is_none() {
            tracing::warn!(
                node = %node_id,
                rack = %rack_id,
                target_user = %username,
                "skipping password rotation due to missing switch credentials"
            );

            let error_message = format!("Missing host credentials for switch {node_id}");

            return rm::NodeOperationResult {
                node_id,
                status: rm::ReturnCode::Failure.into(),
                error_message,
            };
        }

        // Password rotation is a host/NVOS workflow. The host endpoint is
        // required for the operation; BMC details are optional and only
        // retained when valid for later Redfish power use. Malformed BMC
        // endpoint configuration is dropped with a warning rather than
        // failing the node.
        let bmc_endpoint = match flat.optional_bmc_endpoint() {
            Ok(endpoint) => endpoint,
            Err(e) => {
                tracing::warn!(
                    node = %node_id,
                    rack = %rack_id,
                    target_user = %username,
                    error = %e.message,
                    "ignoring malformed BMC endpoint for switch password rotation"
                );

                None
            }
        };

        let host_endpoint = match flat.switch_host_management_endpoint() {
            Ok(endpoint) => endpoint,
            Err(e) => {
                let message = e.message;

                tracing::warn!(
                    node = %node_id,
                    rack = %rack_id,
                    target_user = %username,
                    error = %message,
                    "skipping password rotation due to invalid switch host endpoint"
                );

                let error_message =
                    format!("Invalid host endpoint for switch {node_id}: {message}");

                return rm::NodeOperationResult {
                    node_id,
                    status: rm::ReturnCode::Failure.into(),
                    error_message,
                };
            }
        };

        let config = NodeConfig {
            id: node_id.clone(),
            node_type,
            bmc_endpoint,
            host_endpoint: Some(host_endpoint),
        };

        let switch_result = NodeInstance::create_switch_password(&config, &rack_id);

        let mut switch = match switch_result {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(
                    node = %node_id,
                    rack = %rack_id,
                    target_user = %username,
                    error = %e.message,
                    "failed to construct switch for password rotation"
                );

                let error_message = format!("Failed to construct switch: {}", e.message);

                return rm::NodeOperationResult {
                    node_id,
                    status: rm::ReturnCode::Failure.into(),
                    error_message,
                };
            }
        };

        // Use the same NVUE client initialization path as other switch
        // workflows so transport security, including mTLS state, remains
        // centralized in the shared client setup.
        if let Err(e) = self
            .initialize_nvue_client(switch.optional_nvue_client(), None)
            .await
        {
            return rm::NodeOperationResult {
                node_id,
                status: rm::ReturnCode::Failure.into(),
                error_message: e.message,
            };
        }

        tracing::info!(
            node = %node_id,
            rack = %rack_id,
            target_user = %username,
            "starting switch password rotation"
        );

        match switch
            .update_system_user_password_persisted(username, password)
            .await
        {
            Ok(()) => {
                tracing::info!(
                    node = %node_id,
                    rack = %rack_id,
                    target_user = %username,
                    "completed switch password rotation"
                );

                rm::NodeOperationResult {
                    node_id,
                    status: rm::ReturnCode::Success.into(),
                    error_message: String::new(),
                }
            }
            Err(e) => {
                let error_message = redact_password_rotation_error(&e.message);

                tracing::error!(
                    node = %node_id,
                    rack = %rack_id,
                    target_user = %username,
                    error = %error_message,
                    "switch password rotation failed"
                );

                rm::NodeOperationResult {
                    node_id,
                    status: rm::ReturnCode::Failure.into(),
                    error_message,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_info(rack_id: &str, node_id: &str) -> rm::NodeInfo {
        rm::NodeInfo {
            rack_id: rack_id.to_string(),
            node_id: node_id.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn redact_password_rotation_error_masks_password_fields() {
        let message = r#"revision failed: {"hashed-password":"$6$salt$hash","password":"secret"} password=plain"#;

        let redacted = redact_password_rotation_error(message);

        assert!(!redacted.contains("$6$salt$hash"));
        assert!(!redacted.contains("secret"));
        assert!(!redacted.contains("plain"));
        assert!(redacted.contains("hashed-password"));
        assert!(redacted.contains("password"));
    }

    #[test]
    fn validate_unique_switch_password_targets_rejects_duplicate_rack_node() {
        let devices = vec![
            node_info("rack-01", "node-01"),
            node_info("rack-01", "node-02"),
            node_info("rack-01", "node-01"),
        ];

        let error = validate_unique_switch_password_targets(&devices).unwrap_err();

        assert_eq!(error.code(), tonic::Code::InvalidArgument);

        assert!(
            error
                .message()
                .contains("duplicate switch target in request: rack_id=rack-01 node_id=node-01")
        );
    }

    #[test]
    fn validate_unique_switch_password_targets_allows_same_node_in_different_racks() {
        let devices = vec![
            node_info("rack-01", "node-01"),
            node_info("rack-02", "node-01"),
        ];

        validate_unique_switch_password_targets(&devices).unwrap();
    }
}
