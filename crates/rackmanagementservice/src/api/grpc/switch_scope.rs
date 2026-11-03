/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Shared switch admission across registered, direct, and managed operations.

use crate::api::grpc::conversions::{EndpointKind, FlatEndpoint};
use crate::api::grpc::server::RackManagerServiceImpl;
use crate::domain::node::Node;
use crate::orchestrator::fabric_operation::FabricOperationRegistry;
use crate::orchestrator::job_lifecycle::{JobError, JobFailure, JobScope, MutationReservation};
use crate::orchestrator::job_tracker::{RmsJobDomain, RmsJobHandle};
use crate::utilities::error::{ErrorCode, Result, RmsError};

use librms::protos::rack_manager as rm;
use nvue_client::{ClientEndpoint, DEFAULT_TIMEOUT, SharedClient};

/// SSH recovery uses the fixed destination port in `SshClient::connect`.
const SWITCH_SSH_PORT: u16 = 22;

/// Encodes normalized typed aliases for the process-local operation registry.
///
/// JSON preserves the enum and field boundaries; keys are sorted and deduplicated.
pub(crate) fn fabric_scope_keys(scopes: &[JobScope]) -> Result<Vec<String>> {
    let mut keys = scopes
        .iter()
        .cloned()
        .map(JobScope::normalized)
        .map(|scope| serde_json::to_string(&scope))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            RmsError::internal(format!("cannot encode switch admission aliases: {error}"))
        })?;

    keys.sort();
    keys.dedup();

    Ok(keys)
}

/// Combines observed chassis placement with the NVUE connection destination.
///
/// The URL/TLS host may be a shared certificate name, not a device identity.
pub(crate) fn switch_scopes_for_placement(
    endpoint: ClientEndpoint,
    rack_id: &str,
    node_id: &str,
    chassis_serial_number: &str,
    tray_index: u32,
) -> Vec<JobScope> {
    vec![
        JobScope::LogicalNode {
            rack_id: rack_id.to_owned(),
            node_id: node_id.to_owned(),
        },
        JobScope::PhysicalSwitch {
            chassis_serial_number: chassis_serial_number.trim().to_owned(),
            tray_index,
        },
        JobScope::endpoint(endpoint.connect_host, endpoint.port),
    ]
}

/// Uses the connection destination when the device cannot report placement.
///
/// Endpoint-only admission cannot identify unobserved aliases of the same switch.
pub(crate) fn known_switch_scopes(
    client: Option<&SharedClient>,
    rack_id: &str,
    node_id: &str,
) -> Vec<JobScope> {
    let mut scopes = vec![JobScope::LogicalNode {
        rack_id: rack_id.to_owned(),
        node_id: node_id.to_owned(),
    }];

    if let Some(client) = client {
        let endpoint = client.endpoint();
        scopes.push(JobScope::endpoint(&endpoint.connect_host, endpoint.port));
        scopes.push(JobScope::endpoint(endpoint.connect_host, SWITCH_SSH_PORT));
    }

    scopes
}

/// Extracts connection destinations supplied for host, BMC, and SSH recovery.
///
/// Optional hostnames select TLS authorities; transport uses the supplied IP.
/// Endpoints without an address contribute no alias and are skipped before port validation.
pub(crate) fn request_switch_scopes(node: &rm::NodeInfo) -> Result<Vec<JobScope>> {
    let mut scopes = known_switch_scopes(None, &node.rack_id, &node.node_id);

    let endpoints = node
        .host_endpoint
        .iter()
        .map(|endpoint| (endpoint, EndpointKind::Host))
        .chain(
            node.bmc_endpoint
                .iter()
                .map(|endpoint| (endpoint, EndpointKind::Bmc)),
        )
        .chain(
            node.additional_host_endpoints
                .iter()
                .map(|endpoint| (endpoint, EndpointKind::SupplementalHost)),
        );

    for (endpoint, kind) in endpoints {
        let Some(interface) = &endpoint.interface else {
            continue;
        };

        if interface.ip_address.trim().is_empty() {
            continue;
        }

        let port = if endpoint.port == 0 {
            match kind {
                EndpointKind::Host | EndpointKind::Bmc => 443,
                EndpointKind::SupplementalHost => 22,
            }
        } else {
            u16::try_from(endpoint.port)
                .map_err(|_| RmsError::invalid_argument("switch endpoint port is out of range"))?
        };

        let port = match kind {
            EndpointKind::Host => FlatEndpoint::switch_management_port(port),
            EndpointKind::Bmc | EndpointKind::SupplementalHost => port,
        };

        scopes.push(JobScope::endpoint(&interface.ip_address, port));

        if kind == EndpointKind::Host {
            scopes.push(JobScope::endpoint(&interface.ip_address, SWITCH_SSH_PORT));
        }
    }

    Ok(scopes)
}

/// Extracts configured NVUE, BMC, and SSH destinations without device I/O.
pub(crate) fn configured_switch_scopes(
    node: &impl Node,
    client: Option<&SharedClient>,
) -> Result<Vec<JobScope>> {
    let mut scopes = known_switch_scopes(client, node.rack_id(), node.id());
    let info = node.get_info();

    if client.is_none()
        && let Some(host) = info.get("hostIp_0").filter(|host| !host.trim().is_empty())
    {
        scopes.push(JobScope::endpoint(host, SWITCH_SSH_PORT));
    }

    if let Some(host) = info.get("host") {
        let port = info
            .get("port")
            .map_or(Ok(443), |port| port.parse::<u16>())
            .map_err(|_| RmsError::internal("registered switch reports an invalid BMC port"))?;

        scopes.push(JobScope::endpoint(host, port));
    }

    Ok(scopes)
}

/// Reads physical identity and returns an error when it is unavailable or incomplete.
///
/// Discovery errors do not by themselves reject recovery workflows, which retain
/// known logical and endpoint reservations. Ownership conflicts still reject mutation.
pub(crate) async fn discover_switch_scopes(
    client: &SharedClient,
    rack_id: &str,
    node_id: &str,
) -> Result<Vec<JobScope>> {
    let location = client
        .get_chassis_location(DEFAULT_TIMEOUT)
        .await
        .map_err(|error| {
            RmsError::failed_precondition(format!(
                "cannot read switch '{node_id}' mutation identity: {error}"
            ))
        })?;

    let serial = location
        .chassis_sn
        .filter(|serial| !serial.trim().is_empty())
        .ok_or_else(|| {
            RmsError::failed_precondition(format!("switch '{node_id}' reports no chassis identity"))
        })?;

    let tray_index = location
        .tray_index
        .as_deref()
        .and_then(|tray| tray.trim().parse().ok())
        .ok_or_else(|| {
            RmsError::failed_precondition(format!("switch '{node_id}' reports no valid tray index"))
        })?;

    Ok(switch_scopes_for_placement(
        client.endpoint(),
        rack_id,
        node_id,
        &serial,
        tray_index,
    ))
}

/// Returns known aliases and adds physical identity when discovery succeeds.
///
/// Existing mutations do not require chassis metadata. Discovery failure falls
/// back to logical and endpoint identities; callers still enforce reservations.
pub(crate) async fn switch_scopes_for_mutation(
    client: &SharedClient,
    rack_id: &str,
    node_id: &str,
) -> Vec<JobScope> {
    discover_switch_scopes(client, rack_id, node_id)
        .await
        .unwrap_or_else(|_| known_switch_scopes(Some(client), rack_id, node_id))
}

/// Rejects active or unresolved operations and invalidates stale activation proofs.
///
/// The caller retains alias admission throughout this check and the mutation.
pub(crate) fn require_no_fabric_operation(
    registry: &FabricOperationRegistry,
    scopes: &[JobScope],
) -> Result<()> {
    let keys = fabric_scope_keys(scopes)?;
    registry.invalidate_and_require_clear(&keys)
}

impl RackManagerServiceImpl {
    /// Reserves supplied aliases for the job and checks fabric ownership without device I/O.
    /// Reservations remain job-owned until the worker records its terminal outcome.
    pub(crate) fn admit_known_switch_job_scopes(
        &self,
        job: &RmsJobHandle,
        scopes: Vec<JobScope>,
    ) -> std::result::Result<(), JobFailure> {
        self.job_tracker
            .registry()
            .reserve_job_scopes(job.id(), scopes.clone())
            .map_err(|failure| {
                if job.cancellation_token().is_cancelled() {
                    JobFailure::new(
                        JobError::Other,
                        "switch mutation cancelled before alias admission",
                    )
                } else {
                    failure
                }
            })?;

        require_no_fabric_operation(self.fabric_operations.as_ref(), &scopes).map_err(|error| {
            let code = match error.code {
                ErrorCode::AlreadyExists => JobError::UpdateInProgress,
                ErrorCode::InvalidArgument => JobError::InvalidArgument,
                ErrorCode::Cancelled => JobError::Other,
                _ => JobError::Internal,
            };

            JobFailure::new(code, error.message)
        })
    }

    /// Holds known aliases and any discovered identity before a direct mutation.
    pub(crate) async fn reserve_switch_mutation(
        &self,
        client: Option<&SharedClient>,
        node: &rm::NodeInfo,
    ) -> Result<MutationReservation<RmsJobDomain>> {
        let mut scopes = request_switch_scopes(node)?;

        // Request aliases remain reserved even when no NVUE client is available.
        if let Some(client) = client {
            scopes.extend(switch_scopes_for_mutation(client, &node.rack_id, &node.node_id).await);
        }

        let reservation = self
            .job_tracker
            .reserve_mutation(scopes.clone())
            .map_err(|failure| RmsError::failed_precondition(failure.message))?;

        require_no_fabric_operation(self.fabric_operations.as_ref(), &scopes)?;

        Ok(reservation)
    }

    /// Attaches supplied and discovered aliases before the job's first switch mutation.
    ///
    /// Aliases remain job-owned during device I/O and compensation, including
    /// registry conflicts. The worker records its terminal outcome afterward.
    pub(crate) async fn admit_switch_job_scopes(
        &self,
        job: &RmsJobHandle,
        client: Option<&SharedClient>,
        rack_id: &str,
        node_id: &str,
        mut scopes: Vec<JobScope>,
    ) -> std::result::Result<(), JobFailure> {
        scopes.extend(if let Some(client) = client {
            switch_scopes_for_mutation(client, rack_id, node_id).await
        } else {
            known_switch_scopes(None, rack_id, node_id)
        });

        self.admit_known_switch_job_scopes(job, scopes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use crate::orchestrator::fabric_operation::FabricOperationIntent;
    use crate::utilities::error::ErrorCode;

    use nvue_client::{Client, ClientConfig, ClientCredentials};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn canonical_scope_keys_preserve_typed_identity_and_normalize_aliases() {
        let scopes = vec![
            JobScope::Endpoint {
                host: "SWITCH.EXAMPLE.".to_owned(),
                port: 443,
            },
            JobScope::endpoint("switch.example", 443),
        ];

        let keys = fabric_scope_keys(&scopes).unwrap();

        assert_eq!(
            keys,
            vec![r#"{"Endpoint":{"host":"switch.example","port":443}}"#]
        );

        assert_ne!(
            fabric_scope_keys(&[JobScope::LogicalNode {
                rack_id: "a:b".to_owned(),
                node_id: "c".to_owned()
            }])
            .unwrap(),
            fabric_scope_keys(&[JobScope::LogicalNode {
                rack_id: "a".to_owned(),
                node_id: "b:c".to_owned()
            }])
            .unwrap()
        );
    }

    #[tokio::test]
    async fn switch_discovery_requires_live_physical_identity_and_actual_endpoint() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/platform/chassis-location"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({"chassis-sn":"chassis-a", "tray-index":"2"})),
            )
            .expect(1)
            .mount(&server)
            .await;

        let client = Arc::new(
            Client::new(ClientConfig {
                endpoint: ClientEndpoint::http("127.0.0.1", server.address().port()),
                credentials: ClientCredentials::new("test", "test"),
                dangerously_accept_invalid_certs: false,
            })
            .unwrap(),
        );

        let scopes = discover_switch_scopes(&client, "rack-a", "caller-label")
            .await
            .unwrap();

        assert!(scopes.contains(&JobScope::PhysicalSwitch {
            chassis_serial_number: "chassis-a".to_owned(),
            tray_index: 2
        }));

        assert!(scopes.contains(&JobScope::endpoint("127.0.0.1", server.address().port())));
    }

    #[test]
    fn placement_uses_socket_identity_without_collapsing_switch_trays() {
        let mut endpoint = ClientEndpoint::http("192.0.2.10", 8080);
        endpoint.host = "SWITCH.EXAMPLE.".to_owned();
        let scopes = switch_scopes_for_placement(endpoint, "rack-a", "switch-a", " serial-a ", 3);

        assert!(scopes.contains(&JobScope::endpoint("192.0.2.10", 8080)));
        assert!(!scopes.contains(&JobScope::endpoint("switch.example", 8080)));

        assert!(scopes.contains(&JobScope::PhysicalSwitch {
            chassis_serial_number: "serial-a".to_owned(),
            tray_index: 3,
        }));

        assert!(!scopes.contains(&JobScope::PhysicalSwitch {
            chassis_serial_number: "serial-a".to_owned(),
            tray_index: 4,
        }));
    }

    #[test]
    fn distinct_switches_sharing_a_tls_authority_do_not_conflict() {
        let tracker = crate::orchestrator::job_tracker::JobTracker::default();

        let mut reservations = Vec::new();

        for (ip, rack, node, serial) in [
            ("192.0.2.1", "rack-a", "switch-a", "chassis-a"),
            ("192.0.2.2", "rack-b", "switch-b", "chassis-b"),
        ] {
            let endpoint = ClientEndpoint::https("shared.example", ip, 443);

            let client = Client::new(ClientConfig {
                endpoint: endpoint.clone(),
                credentials: ClientCredentials::new("test", "test"),
                dangerously_accept_invalid_certs: false,
            })
            .unwrap();

            let mut scopes = switch_scopes_for_placement(endpoint, rack, node, serial, 0);
            scopes.extend(known_switch_scopes(Some(&client), rack, node));

            let request = rm::NodeInfo {
                rack_id: rack.to_owned(),
                node_id: node.to_owned(),
                host_endpoint: Some(rm::Endpoint {
                    interface: Some(rm::NetworkInterface {
                        ip_address: ip.to_owned(),
                        host_name: Some("shared.example".to_owned()),
                        ..Default::default()
                    }),
                    port: 443,
                    ..Default::default()
                }),
                ..Default::default()
            };

            scopes.extend(request_switch_scopes(&request).unwrap());

            let reservation = tracker.reserve_mutation(scopes).unwrap();
            reservations.push(reservation);
        }

        assert!(
            tracker
                .reserve_mutation(vec![JobScope::endpoint("192.0.2.1", 443)])
                .is_err()
        );

        assert!(
            tracker
                .reserve_mutation(switch_scopes_for_placement(
                    ClientEndpoint::https("other.example", "192.0.2.3", 443),
                    "rack-c",
                    "switch-c",
                    "chassis-a",
                    0,
                ))
                .is_err()
        );

        drop(reservations);

        assert!(
            tracker
                .reserve_mutation(vec![JobScope::endpoint("192.0.2.1", 443)])
                .is_ok()
        );
    }

    #[test]
    fn request_recovery_aliases_match_actual_management_and_ssh_ports() {
        let endpoint = |host: &str, port| rm::Endpoint {
            interface: Some(rm::NetworkInterface {
                ip_address: host.to_owned(),
                host_name: Some("SWITCH.EXAMPLE.".to_owned()),
                ..Default::default()
            }),
            port,
            ..Default::default()
        };

        let node = rm::NodeInfo {
            rack_id: "rack-a".to_owned(),
            node_id: "switch-a".to_owned(),
            host_endpoint: Some(endpoint("192.0.2.10", 22)),
            bmc_endpoint: Some(endpoint("192.0.2.11", 0)),
            additional_host_endpoints: vec![endpoint("192.0.2.12", 0)],
            ..Default::default()
        };

        let scopes = request_switch_scopes(&node).unwrap();

        assert!(scopes.contains(&JobScope::endpoint("192.0.2.10", 443)));
        assert!(scopes.contains(&JobScope::endpoint("192.0.2.11", 443)));
        assert!(scopes.contains(&JobScope::endpoint("192.0.2.12", 22)));
        assert!(!scopes.contains(&JobScope::endpoint("switch.example", 443)));
        assert!(scopes.contains(&JobScope::endpoint("192.0.2.10", 22)));

        assert!(
            !scopes
                .iter()
                .any(|scope| matches!(scope, JobScope::PhysicalSwitch { .. }))
        );

        let tracker = crate::orchestrator::job_tracker::JobTracker::default();
        let reservation = tracker.reserve_mutation(scopes).unwrap();

        assert!(
            tracker
                .reserve_mutation(vec![JobScope::endpoint("192.0.2.10", 443)])
                .is_err()
        );

        assert!(
            tracker
                .reserve_mutation(vec![JobScope::endpoint("192.0.2.11", 443)])
                .is_err()
        );

        assert!(
            tracker
                .reserve_mutation(vec![JobScope::endpoint("192.0.2.10", 22)])
                .is_err()
        );

        drop(reservation);

        assert!(
            tracker
                .reserve_mutation(vec![JobScope::endpoint("192.0.2.11", 443)])
                .is_ok()
        );
    }

    #[test]
    fn request_aliases_skip_missing_addresses_before_validating_ports() {
        for ip in [None, Some(""), Some(" \t "), Some("192.0.2.10")] {
            let endpoint = rm::Endpoint {
                interface: ip.map(|ip| rm::NetworkInterface {
                    ip_address: ip.into(),
                    ..Default::default()
                }),
                port: 65_536,
                ..Default::default()
            };

            let node = rm::NodeInfo {
                rack_id: "rack-a".into(),
                node_id: "switch-a".into(),
                additional_host_endpoints: vec![endpoint],
                ..Default::default()
            };

            let result = request_switch_scopes(&node);

            if ip == Some("192.0.2.10") {
                assert_eq!(result.unwrap_err().code, ErrorCode::InvalidArgument);
            } else {
                assert_eq!(
                    result.unwrap(),
                    vec![JobScope::LogicalNode {
                        rack_id: node.rack_id,
                        node_id: node.node_id,
                    }]
                );
            }
        }
    }

    #[test]
    fn request_aliases_normalize_only_host_ssh_ports() {
        for port in [0, 22, 443, 8443, u32::from(u16::MAX)] {
            let endpoint = rm::Endpoint {
                interface: Some(rm::NetworkInterface {
                    ip_address: "192.0.2.10".into(),
                    ..Default::default()
                }),
                port,
                ..Default::default()
            };

            for (kind, expected) in [
                (
                    EndpointKind::Host,
                    if port == 0 || port == 22 { 443 } else { port },
                ),
                (EndpointKind::Bmc, if port == 0 { 443 } else { port }),
                (
                    EndpointKind::SupplementalHost,
                    if port == 0 { 22 } else { port },
                ),
            ] {
                let mut node = rm::NodeInfo::default();

                match kind {
                    EndpointKind::Host => node.host_endpoint = Some(endpoint.clone()),
                    EndpointKind::Bmc => node.bmc_endpoint = Some(endpoint.clone()),
                    EndpointKind::SupplementalHost => {
                        node.additional_host_endpoints.push(endpoint.clone());
                    }
                }

                let scopes = request_switch_scopes(&node).unwrap();

                assert!(scopes.contains(&JobScope::endpoint("192.0.2.10", expected as u16)));
            }
        }
    }

    #[tokio::test]
    async fn switch_discovery_rejects_incomplete_physical_identity() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/platform/chassis-location"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "chassis-sn": "serial-a",
            })))
            .mount(&server)
            .await;

        let client = Arc::new(
            Client::new(ClientConfig {
                endpoint: ClientEndpoint::http("127.0.0.1", server.address().port()),
                credentials: ClientCredentials::new("test", "test"),
                dangerously_accept_invalid_certs: false,
            })
            .unwrap(),
        );

        let error = discover_switch_scopes(&client, "rack-a", "switch-a")
            .await
            .unwrap_err();

        assert_eq!(error.code, ErrorCode::FailedPrecondition);
        assert!(error.message.contains("no valid tray index"));
    }

    #[test]
    fn direct_mutation_rejects_active_coordinator_scope() {
        let registry = FabricOperationRegistry::default();
        let scopes = vec![JobScope::endpoint("switch.example", 443)];

        let intent = FabricOperationIntent {
            scope_keys: fabric_scope_keys(&scopes).unwrap(),
            rule_id: "rule-a".to_owned(),
        };

        let _admission = registry.admit(intent, "job-a", None).unwrap();

        let error = require_no_fabric_operation(&registry, &scopes).unwrap_err();

        assert_eq!(error.code, ErrorCode::AlreadyExists);

        assert_eq!(
            error.message,
            "fabric operation owns external mutation scope"
        );

        assert!(
            require_no_fabric_operation(&registry, &[JobScope::endpoint("switch-b", 443)]).is_ok()
        );
    }

    #[tokio::test]
    async fn mutation_discovery_retains_known_aliases_without_chassis_metadata() {
        for (status, body, physical) in [
            (404, serde_json::json!({}), false),
            (200, serde_json::json!({"chassis-sn": "serial-a"}), false),
            (
                200,
                serde_json::json!({"chassis-sn": "serial-a", "tray-index": "1"}),
                true,
            ),
        ] {
            let server = MockServer::start().await;

            Mock::given(method("GET"))
                .and(path("/nvue_v1/platform/chassis-location"))
                .respond_with(ResponseTemplate::new(status).set_body_json(body))
                .expect(1)
                .mount(&server)
                .await;

            let client = Arc::new(
                Client::new(ClientConfig {
                    endpoint: ClientEndpoint::http("127.0.0.1", server.address().port()),
                    credentials: ClientCredentials::new("test", "test"),
                    dangerously_accept_invalid_certs: false,
                })
                .unwrap(),
            );

            let scopes = switch_scopes_for_mutation(&client, "rack-a", "switch-a").await;
            let tracker = crate::orchestrator::job_tracker::JobTracker::default();
            let _reservation = tracker.reserve_mutation(scopes.clone()).unwrap();

            assert!(scopes.contains(&JobScope::LogicalNode {
                rack_id: "rack-a".into(),
                node_id: "switch-a".into(),
            }));

            assert!(
                tracker
                    .reserve_mutation(vec![JobScope::endpoint(
                        "127.0.0.1",
                        server.address().port()
                    )])
                    .is_err()
            );

            let physical_scope = JobScope::PhysicalSwitch {
                chassis_serial_number: "serial-a".into(),
                tray_index: 1,
            };

            assert_eq!(scopes.contains(&physical_scope), physical);

            assert_eq!(
                tracker.reserve_mutation(vec![physical_scope]).is_err(),
                physical
            );

            server.verify().await;
        }
    }
}
