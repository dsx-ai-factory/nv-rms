/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Managed fabric admission and coordinated reconciliation.
//!
//! Discovery and planning finish before job admission. One worker owns the
//! complete switch inventory, prepares shared links, then activates the resolved
//! controllers. Live status is collected independently by the inspection RPC.

mod controller_targets;
mod links;
pub(super) mod observations;
mod reconciliation;

use std::collections::BTreeSet;

use super::layout_planner as planner;
use crate::api::grpc::server::RackManagerServiceImpl;
use crate::api::grpc::switch_scope::request_switch_scopes;
use crate::config::fabric_layout::{validate_static_config_key, validate_static_config_value};
use crate::domain::node::{NodeKind, NodeType};
use crate::orchestrator::fabric_operation::FabricOperationIntent;
use crate::orchestrator::job_lifecycle::{JobError, JobFailure, JobScope};
use crate::orchestrator::job_tracker::{JobType, job_error_of};
use crate::utilities::error::ErrorCode;

use librms::protos::rack_manager as rm;
use librms::protos::rack_manager_v2 as rm_v2;

/// Validated request settings and inventories separated by hardware role.
#[derive(Debug)]
struct ValidatedLayoutRequest<'a> {
    layout: &'a rm_v2::ScaleUpFabricLayout,
    config: &'a rm_v2::ScaleUpFabricConfig,
    switches: Vec<rm::NodeInfo>,
    computes: Vec<rm::NodeInfo>,
}

/// Validates caller-owned settings before discovery or complete-file writes.
fn validate_managed_config(
    config: rm_v2::ScaleUpFabricConfig,
) -> Result<rm_v2::ScaleUpFabricConfig, tonic::Status> {
    let mut config = super::validate_scale_up_fabric_config(config)?;
    validate_static_config_value(&config.topology_type).map_err(tonic::Status::invalid_argument)?;

    for entry in &config.extra_static_configs {
        validate_static_config_key(&entry.config_file_name, &entry.key)
            .map_err(tonic::Status::invalid_argument)?;

        validate_static_config_value(&entry.value).map_err(tonic::Status::invalid_argument)?;
    }

    config.extra_static_configs.sort_by(|left, right| {
        (&left.config_file_name, &left.key).cmp(&(&right.config_file_name, &right.key))
    });

    Ok(config)
}

impl RackManagerServiceImpl {
    /// Resolves complete physical scope before admitting one coordinated job.
    ///
    /// Active jobs reject overlapping requests. A terminal unresolved operation
    /// can transfer ownership only when its physical scope and catalog identity match.
    pub(super) async fn handle_configure_fabric_layout(
        &self,
        request: rm_v2::ConfigureScaleUpFabricManagerRequest,
    ) -> Result<tonic::Response<rm_v2::ConfigureScaleUpFabricManagerResponse>, tonic::Status> {
        let (switches, computes, plan) = self.resolve_managed_fabric(&request).await?;

        let scopes: Vec<_> = switches
            .iter()
            .map(observations::ManagedSwitch::scopes)
            .collect::<Result<Vec<_>, _>>()
            .map_err(tonic::Status::failed_precondition)?
            .into_iter()
            .flatten()
            .collect();

        // Caller labels remain job aliases, but cannot change physical ownership
        // when a retry supplies different logical node or rack names.
        let canonical_scopes: Vec<_> = scopes
            .iter()
            .filter(|scope| !matches!(scope, JobScope::LogicalNode { .. }))
            .cloned()
            .collect();

        let scope_keys = crate::api::grpc::switch_scope::fabric_scope_keys(&canonical_scopes)
            .map_err(|_| tonic::Status::internal("cannot encode managed switch scope"))?;

        let intent = FabricOperationIntent {
            scope_keys,
            rule_id: self.fabric_layout_catalog.identity().to_owned(),
        };

        let overlapping = self
            .fabric_operations
            .find_overlapping(&intent.scope_keys)
            .map_err(|_| tonic::Status::internal("cannot read coordinator ownership"))?;

        let selected_fabrics = plan
            .fabrics
            .iter()
            .map(|fabric| fabric.assignment.clone())
            .collect();

        if overlapping.len() > 1 {
            return Err(tonic::Status::failed_precondition(
                "multiple unresolved operations overlap the requested physical scope",
            ));
        }

        if let Some(previous) = overlapping.first()
            && self
                .job_tracker
                .get_job(&previous.job_id)
                .is_some_and(|job| !job.state.is_terminal())
        {
            return Err(tonic::Status::failed_precondition(
                "an active job owns the requested physical fabric scope",
            ));
        }

        let rack_id = switches
            .first()
            .map(|switch| switch.node.rack_id.as_str())
            .unwrap_or_default();

        let job = self
            .job_tracker
            .create_scoped_job(
                rack_id,
                "",
                JobType::ConfigureScaleUpFabricManagerV2,
                scopes,
            )
            .map_err(|failure| match job_error_of(&failure.error) {
                JobError::UpdateInProgress => tonic::Status::failed_precondition(failure.message),
                _ => tonic::Status::internal(failure.message),
            })?;

        let job_id = job.id().to_string();

        let operation = match self.fabric_operations.admit(
            intent,
            &job_id,
            overlapping
                .first()
                .map(|operation| operation.job_id.as_str()),
        ) {
            Ok(operation) => operation,
            Err(error) => {
                let (job_error, status) = if error.code == ErrorCode::Internal {
                    (JobError::Internal, tonic::Status::internal(error.message))
                } else {
                    (
                        JobError::FailedPrecondition,
                        tonic::Status::failed_precondition(error.message),
                    )
                };

                job.fail(JobFailure::new(
                    job_error,
                    "cannot acquire fabric coordinator ownership",
                ));

                return Err(status);
            }
        };

        let service = self.clone();

        self.job_tracker
            .spawn_job(job, move |job| async move {
                service
                    .run_managed_fabric_job(
                        job,
                        operation,
                        switches,
                        computes,
                        plan,
                        request.domain,
                    )
                    .await;
            })
            .detach();

        Ok(tonic::Response::new(
            rm_v2::ConfigureScaleUpFabricManagerResponse {
                job_id,
                selected_fabrics,
            },
        ))
    }

    /// Discovers placement and validates an explicit deployment target without device writes.
    ///
    /// Every physical switch is required so unassigned switches can be isolated.
    /// Compute discovery uses BMCs for smaller layouts; a full-capacity target
    /// can omit compute endpoints. Missing catalog matches or placement errors
    /// reject the request before job creation.
    async fn resolve_managed_fabric(
        &self,
        request: &rm_v2::ConfigureScaleUpFabricManagerRequest,
    ) -> Result<
        (
            Vec<observations::ManagedSwitch>,
            Vec<planner::ObservedPlacement>,
            planner::FabricPlan,
        ),
        tonic::Status,
    > {
        let ValidatedLayoutRequest {
            layout,
            config,
            switches: nodes,
            computes: compute_nodes,
        } = validate_layout_request(request)?;

        let config = validate_managed_config(config.clone())?;

        if self.fabric_layout_catalog.identity().is_empty() {
            return Err(tonic::Status::unimplemented(
                "managed fabric reconciliation has no installed deployment catalog",
            ));
        }

        self.validate_scale_up_fabric_nodes(&nodes, request.domain.as_deref())
            .await?;

        let switches = self
            .discover_layout_switches(&nodes, request.domain.as_deref())
            .await
            .map_err(tonic::Status::failed_precondition)?;

        let switch_locations: Vec<_> = switches
            .iter()
            .map(|switch| switch.placement.clone())
            .collect();

        let first = switch_locations
            .first()
            .ok_or_else(|| tonic::Status::invalid_argument("switch inventory is empty"))?;

        let physical_topology = first.physical_topology.as_deref().ok_or_else(|| {
            tonic::Status::failed_precondition("physical switch topology is unknown")
        })?;

        let types = switch_locations
            .iter()
            .map(|node| node.node_type.clone())
            .collect();

        let profile = self
            .fabric_layout_catalog
            .select(physical_topology, &types)
            .ok_or_else(|| {
                tonic::Status::failed_precondition(
                    "observed topology has no matching deployment rules",
                )
            })?;

        // Reject excess inventory before opening any compute BMC connections.
        if compute_nodes.len() as u64 > u64::from(profile.compute_tray_count) {
            return Err(tonic::Status::invalid_argument(
                "compute inventory exceeds the selected profile's capacity",
            ));
        }

        let computes = self
            .discover_layout_computes(&compute_nodes)
            .await
            .map_err(tonic::Status::failed_precondition)?;

        let plan = planner::plan_layout(
            profile,
            &switch_locations,
            &computes,
            &layout.fabrics,
            &config,
        )
        .map_err(tonic::Status::failed_precondition)?;

        Ok((switches, computes, plan))
    }
}

/// Validates the shared inventory and separates nodes by their resolved roles.
///
/// Compute omission is checked against the physical profile during planning.
/// Caller labels and inventory order do not determine placement.
fn validate_layout_request(
    request: &rm_v2::ConfigureScaleUpFabricManagerRequest,
) -> Result<ValidatedLayoutRequest<'_>, tonic::Status> {
    let layout = request
        .layout
        .as_ref()
        .ok_or_else(|| tonic::Status::invalid_argument("layout is required"))?;

    let Some(config) = request
        .config
        .as_ref()
        .filter(|_| request.primary_switch_node_id.is_none())
    else {
        return Err(tonic::Status::invalid_argument(
            "layout requires config and an absent top-level primary_switch_node_id",
        ));
    };

    let nodes = request
        .nodes
        .as_ref()
        .map(|set| set.nodes.as_slice())
        .unwrap_or_default();

    if layout.fabrics.is_empty() {
        return Err(tonic::Status::invalid_argument(
            "layout requires a nonempty fabric list",
        ));
    }

    let types = super::inventory::validate(nodes)?;

    let mut switches = Vec::new();
    let mut computes = Vec::new();

    for (node, node_type) in nodes.iter().zip(types) {
        // VR requires per-group node-IP reconciliation before controller startup,
        // which the managed workflow does not implement.
        if node_type.kind() == NodeKind::Switch
            && !matches!(
                node_type,
                NodeType::SwitchGb200Nvidia | NodeType::SwitchGb300Nvidia
            )
        {
            return Err(tonic::Status::unimplemented(
                "managed fabric layouts support only GB200 and GB300 switches",
            ));
        }

        if node.node_descriptor.as_ref().is_some_and(|descriptor| {
            descriptor
                .attributes
                .keys()
                .any(|key| key.starts_with("fm_config:"))
        }) {
            return Err(tonic::Status::invalid_argument(
                "layout does not accept descriptor-backed fabric-manager overrides",
            ));
        }

        match node_type.kind() {
            NodeKind::Switch => {
                request_switch_scopes(node)
                    .map_err(|error| tonic::Status::invalid_argument(error.message))?;

                switches.push(node.clone());
            }
            NodeKind::Compute => computes.push(node.clone()),
            NodeKind::Powershelf => {
                return Err(tonic::Status::invalid_argument(
                    "layout accepts only switch and compute nodes",
                ));
            }
        }
    }

    if switches.is_empty() {
        return Err(tonic::Status::invalid_argument(
            "layout requires at least one switch",
        ));
    }

    let switch_inventory = switches
        .iter()
        .map(|node| node.node_id.as_str())
        .collect::<BTreeSet<_>>();

    let mut primaries = BTreeSet::new();
    let mut switch_count = 0_u64;
    let mut compute_count = 0_u64;

    for fabric in &layout.fabrics {
        if fabric.compute_tray_count == 0 || fabric.switch_tray_count == 0 {
            return Err(tonic::Status::invalid_argument(
                "fabric tray counts must be positive",
            ));
        }

        switch_count += u64::from(fabric.switch_tray_count);
        compute_count += u64::from(fabric.compute_tray_count);

        if switch_count > switches.len() as u64
            || (!computes.is_empty() && compute_count > computes.len() as u64)
        {
            return Err(tonic::Status::invalid_argument(
                "fabric counts exceed the supplied inventory",
            ));
        }

        if fabric.primary_switch_node_id.as_ref().is_some_and(|id| {
            !switch_inventory.contains(id.as_str()) || !primaries.insert(id.as_str())
        }) {
            return Err(tonic::Status::invalid_argument(
                "preferred primaries must exist in switch inventory and be distinct",
            ));
        }
    }

    Ok(ValidatedLayoutRequest {
        layout,
        config,
        switches,
        computes,
    })
}

#[cfg(test)]
mod admission_tests;

#[cfg(test)]
mod tests {
    use super::*;

    use librms::protos::rack_manager as rm;

    fn request() -> rm_v2::ConfigureScaleUpFabricManagerRequest {
        let node = |id: &str, kind: rm::NodeType| rm::NodeInfo {
            node_id: id.to_owned(),
            r#type: Some(kind as i32),
            ..Default::default()
        };

        rm_v2::ConfigureScaleUpFabricManagerRequest {
            nodes: Some(rm::NodeSet {
                nodes: vec![
                    node("sw-a", rm::NodeType::SwitchGb200Nvidia),
                    node("compute-a", rm::NodeType::ComputeGb200Nvidia),
                ],
            }),
            config: Some(rm_v2::ScaleUpFabricConfig {
                topology_type: "synthetic-fabric".into(),
                ..Default::default()
            }),
            layout: Some(rm_v2::ScaleUpFabricLayout {
                fabrics: vec![rm_v2::ScaleUpFabricSpec {
                    compute_tray_count: 1,
                    switch_tray_count: 1,
                    ..Default::default()
                }],
            }),
            ..Default::default()
        }
    }

    #[test]
    fn separates_mixed_inventory_using_descriptor_roles() {
        let mut request = request();
        let nodes = &mut request.nodes.as_mut().unwrap().nodes;

        nodes.reverse();
        nodes[0].r#type = None;

        nodes[0].node_descriptor = Some(rm::NodeDescriptor {
            attributes: [
                ("role".into(), "compute".into()),
                ("vendor".into(), "nvidia".into()),
                ("product_family".into(), "gb200".into()),
            ]
            .into(),
        });

        let inventory = validate_layout_request(&request).unwrap();

        assert_eq!(inventory.switches.len(), 1);
        assert_eq!(inventory.switches[0].node_id, "sw-a");
        assert_eq!(inventory.computes.len(), 1);
        assert_eq!(inventory.computes[0].node_id, "compute-a");
    }

    #[test]
    fn accepts_automatic_selection_and_primary_preference() {
        let mut request = request();

        assert!(validate_layout_request(&request).is_ok());

        let fabric = &mut request.layout.as_mut().unwrap().fabrics[0];
        fabric.primary_switch_node_id = Some("sw-a".to_owned());

        assert!(validate_layout_request(&request).is_ok());
    }

    #[test]
    fn managed_layout_accepts_gb_switches_and_rejects_vr_before_discovery() {
        for (node_type, expected) in [
            (rm::NodeType::SwitchGb200Nvidia, None),
            (rm::NodeType::SwitchGb300Nvidia, None),
            (
                rm::NodeType::SwitchVrnvl72Nvidia,
                Some(tonic::Code::Unimplemented),
            ),
        ] {
            let mut request = request();
            request.nodes.as_mut().unwrap().nodes[0].r#type = Some(node_type as i32);

            let result = validate_layout_request(&request);

            assert_eq!(
                result.err().map(|error| error.code()),
                expected,
                "{node_type:?}"
            );
        }
    }

    #[test]
    fn rejects_missing_configuration_and_top_level_primary_with_layout() {
        let mut request = request();
        request.config = None;

        assert_eq!(
            validate_layout_request(&request).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        request.config = self::request().config;
        request.primary_switch_node_id = Some("sw-a".to_owned());

        assert_eq!(
            validate_layout_request(&request).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn rejects_missing_inventories_and_empty_fabrics() {
        let mut missing_switches = request();
        missing_switches.nodes = None;
        let mut no_switches = request();
        no_switches.nodes.as_mut().unwrap().nodes.remove(0);
        let mut empty_fabrics = request();
        empty_fabrics.layout.as_mut().unwrap().fabrics.clear();
        let mut missing_layout = request();
        missing_layout.layout = None;

        for request in [missing_switches, no_switches, empty_fabrics, missing_layout] {
            assert_eq!(
                validate_layout_request(&request).unwrap_err().code(),
                tonic::Code::InvalidArgument
            );
        }
    }

    #[test]
    fn omitted_compute_inventory_is_deferred_to_physical_profile_validation() {
        let mut request = request();
        request.nodes.as_mut().unwrap().nodes.pop();

        assert!(validate_layout_request(&request).is_ok());
    }

    #[test]
    fn rejects_duplicate_ids_wrong_roles_and_unknown_types() {
        let mut duplicate = request();
        duplicate.nodes.as_mut().unwrap().nodes[1].node_id = "sw-a".to_owned();

        let mut wrong_role = request();
        wrong_role.nodes.as_mut().unwrap().nodes[1].r#type =
            Some(rm::NodeType::PowershelfGb200Liteon as i32);

        let mut unknown = request();
        unknown.nodes.as_mut().unwrap().nodes[0].r#type = Some(i32::MAX);

        for request in [duplicate, wrong_role, unknown] {
            assert_eq!(
                validate_layout_request(&request).unwrap_err().code(),
                tonic::Code::InvalidArgument
            );
        }
    }

    #[test]
    fn rejects_invalid_switch_endpoint_ports() {
        let mut request = request();

        request.nodes.as_mut().unwrap().nodes[0].additional_host_endpoints = vec![rm::Endpoint {
            interface: Some(rm::NetworkInterface {
                ip_address: "192.0.2.1".into(),
                ..Default::default()
            }),
            port: u32::from(u16::MAX) + 1,
            ..Default::default()
        }];

        assert_eq!(
            validate_layout_request(&request).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn rejects_zero_and_excessive_counts() {
        for count in [0, 2, u32::MAX] {
            let mut request = request();
            request.layout.as_mut().unwrap().fabrics[0].compute_tray_count = count;

            assert_eq!(
                validate_layout_request(&request).unwrap_err().code(),
                tonic::Code::InvalidArgument
            );
        }
    }

    #[test]
    fn rejects_reused_primary_preferences() {
        let mut duplicate = request();

        duplicate.nodes.as_mut().unwrap().nodes.push(rm::NodeInfo {
            node_id: "sw-b".to_owned(),
            r#type: Some(rm::NodeType::SwitchGb200Nvidia as i32),
            ..Default::default()
        });

        duplicate.nodes.as_mut().unwrap().nodes.push(rm::NodeInfo {
            node_id: "compute-b".to_owned(),
            r#type: Some(rm::NodeType::ComputeGb200Nvidia as i32),
            ..Default::default()
        });

        let fabric = &mut duplicate.layout.as_mut().unwrap().fabrics[0];
        fabric.primary_switch_node_id = Some("sw-a".to_owned());
        let fabric = fabric.clone();
        duplicate.layout.as_mut().unwrap().fabrics.push(fabric);

        let error = validate_layout_request(&duplicate).unwrap_err();

        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("distinct"));
    }

    #[test]
    fn rejects_primary_outside_inventory_and_descriptor_fm_overrides() {
        let mut primary = request();
        primary.layout.as_mut().unwrap().fabrics[0].primary_switch_node_id =
            Some("missing".to_owned());

        let mut override_request = request();
        override_request.nodes.as_mut().unwrap().nodes[0].node_descriptor =
            Some(rm::NodeDescriptor {
                attributes: [("fm_config:test_key".to_owned(), "test_value".to_owned())].into(),
            });

        for request in [primary, override_request] {
            assert_eq!(
                validate_layout_request(&request).unwrap_err().code(),
                tonic::Code::InvalidArgument
            );
        }
    }

    #[test]
    fn no_layout_response_preserves_job_only_json() {
        let response = rm_v2::ConfigureScaleUpFabricManagerResponse {
            job_id: "test-job".to_owned(),
            selected_fabrics: Vec::new(),
        };

        let json = serde_json::to_value(response).unwrap();

        assert_eq!(json, serde_json::json!({ "job_id": "test-job" }));
    }

    #[test]
    fn managed_config_rejects_duplicate_keys_and_unsafe_targets() {
        let base = rm_v2::ScaleUpFabricConfig {
            topology_type: "synthetic".into(),
            ..Default::default()
        };

        for targets in [
            vec![("fm_config", "KEY"), ("fm_config", " KEY ")],
            vec![("fm_config", "MNNVL_TOPOLOGY")],
            vec![("FM_CONFIG", "mnnvl_topology")],
            vec![("../file", "KEY")],
            vec![(".hidden", "KEY")],
            vec![("fm_config", "KEY\nOTHER")],
        ] {
            let mut config = base.clone();
            config.extra_static_configs = targets
                .into_iter()
                .map(|(file, key)| rm::ScaleUpFabricStaticConfig {
                    config_file_name: file.into(),
                    key: key.into(),
                    value: "1".into(),
                })
                .collect();

            assert_eq!(
                validate_managed_config(config).unwrap_err().code(),
                tonic::Code::InvalidArgument
            );
        }

        let mut config = base.clone();

        config.extra_static_configs = vec![
            rm::ScaleUpFabricStaticConfig {
                config_file_name: "file".into(),
                key: "KEY".into(),
                value: "1".into(),
            },
            rm::ScaleUpFabricStaticConfig {
                config_file_name: "FILE".into(),
                key: "key".into(),
                value: "1".into(),
            },
        ];

        assert!(validate_managed_config(config.clone()).is_ok());

        config.extra_static_configs[0].value = "target\nOTHER=1".into();

        assert_eq!(
            validate_managed_config(config).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        let mut config = base;
        config.topology_type = "topology\nOTHER=1".into();

        assert!(validate_managed_config(config).is_err());
    }

    #[test]
    fn managed_config_normalizes_and_sorts_settings() {
        let config = rm_v2::ScaleUpFabricConfig {
            topology_type: " synthetic ".into(),
            extra_static_configs: vec![
                rm::ScaleUpFabricStaticConfig {
                    config_file_name: " file ".into(),
                    key: " z ".into(),
                    value: " 1 ".into(),
                },
                rm::ScaleUpFabricStaticConfig {
                    config_file_name: "file".into(),
                    key: "a".into(),
                    value: "2".into(),
                },
            ],
        };

        let normalized = validate_managed_config(config.clone()).unwrap();

        assert_eq!(normalized.topology_type, "synthetic");
        assert_eq!(normalized.extra_static_configs[0].config_file_name, "file");
        assert_eq!(normalized.extra_static_configs[0].key, "a");
        assert_eq!(normalized.extra_static_configs[1].key, "z");
        assert_eq!(normalized.extra_static_configs[1].value, "1");

        let mut reordered = config.clone();
        reordered.extra_static_configs.reverse();

        assert_eq!(validate_managed_config(reordered).unwrap(), normalized);
    }
}
