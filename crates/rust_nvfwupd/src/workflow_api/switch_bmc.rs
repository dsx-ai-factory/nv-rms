// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Direct VR NVL72 switch BMC reads and activation, independent of NVOS/BMC communication.

use std::collections::HashSet;
use std::time::Duration;

use redfish_client::{ODataLink, RedfishClient, RedfishError, RedfishPowerState, ResetType};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use tokio_util::sync::CancellationToken;

use crate::workflow::{ActivationSummary, NvFwUpdError, Result};

const INVENTORY_URI: &str = "/redfish/v1/UpdateService/FirmwareInventory";
const SYSTEM_ID: &str = "System_0";

#[derive(Clone, Deserialize)]
struct InventoryCollection {
    #[serde(rename = "Members")]
    members: Vec<ODataLink>,
    #[serde(rename = "Members@odata.nextLink")]
    next_link: Option<String>,
}

fn redfish_error(error: RedfishError) -> NvFwUpdError {
    let message = error.to_string();
    match error {
        RedfishError::Unauthenticated(_) | RedfishError::Forbidden(_) => NvFwUpdError::AuthFailed {
            target: "switch BMC".to_owned(),
            message,
        },
        _ => NvFwUpdError::Transport {
            operation: "switch BMC Redfish",
            message,
        },
    }
}

fn invalid_inventory(message: impl Into<String>) -> NvFwUpdError {
    NvFwUpdError::InvalidResponse {
        context: "switch BMC firmware inventory",
        message: message.into(),
    }
}

pub(super) async fn firmware_inventory(client: &RedfishClient) -> Result<Map<String, Value>> {
    let mut inventory = Map::new();
    let mut next = Some(INVENTORY_URI.to_owned());
    let mut visited = HashSet::new();
    while let Some(uri) = next {
        if !visited.insert(uri.clone()) || visited.len() > 128 {
            return Err(invalid_inventory(
                "cyclic or excessive inventory pagination",
            ));
        }
        let page: InventoryCollection = client.get_resource(&uri).await.map_err(redfish_error)?;
        for member in page.members {
            let uri = member
                .nonempty()
                .ok_or_else(|| invalid_inventory("empty inventory member URI"))?;
            if inventory.contains_key(uri) {
                continue;
            }
            // The shared client checks each URI's origin before attaching BMC credentials.
            // Do not return a partial inventory when any member cannot be read.
            let details = client.get_json(uri).await.map_err(redfish_error)?;
            inventory.insert(uri.to_owned(), details);
        }
        next = page.next_link.filter(|link| !link.is_empty());
    }
    if inventory.is_empty() {
        return Err(invalid_inventory(
            "BMC reported an empty firmware inventory",
        ));
    }
    Ok(inventory)
}

async fn cancellable<T>(
    cancel: &CancellationToken,
    operation: impl std::future::Future<Output = T>,
) -> Result<T> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(NvFwUpdError::Cancelled { operation: "switch BMC activation" }),
        result = operation => Ok(result),
    }
}

pub(super) async fn activate(
    client: &RedfishClient,
    cancellation: Option<CancellationToken>,
) -> Result<ActivationSummary> {
    let cancel = cancellation.unwrap_or_default();
    cancellable(
        &cancel,
        client.reset_computer_system(SYSTEM_ID, ResetType::PowerCycle),
    )
    .await?
    .map_err(redfish_error)?;
    client.invalidate_cache().await;

    // Avoid declaring recovery from the pre-reset On state. Issue the reset once,
    // allow the tray to restart, then poll only the BMC to avoid a potential
    // BMC/NVOS communication issue.
    #[cfg(not(test))]
    let settle = Duration::from_secs(60);
    #[cfg(test)]
    let settle = Duration::ZERO;
    cancellable(&cancel, tokio::time::sleep(settle)).await?;

    let unreachable_error = |message| NvFwUpdError::BmcUnreachable {
        target: "switch BMC after power cycle".to_owned(),
        message,
    };
    // Retry post-reset authentication failures, but preserve their type if they persist.
    let mut last_error = unreachable_error("system did not report PowerState On".to_owned());
    for attempt in 1..=super::SWITCH_ACTIVATION_RECOVERY_ATTEMPTS {
        let state = cancellable(&cancel, client.computer_system_power_state(SYSTEM_ID)).await?;
        match state {
            Ok(Some(RedfishPowerState::On)) => {
                match cancellable(&cancel, firmware_inventory(client)).await? {
                    Ok(_) => {
                        return Ok(ActivationSummary {
                            message: "switch power cycle via BMC completed".to_owned(),
                            details: json!({
                                "command": "BMC_REDFISH_POWER_CYCLE",
                                "reset_type": "PowerCycle",
                                "recovery": {"endpoint": INVENTORY_URI, "attempts": attempt},
                            }),
                        })
                    }
                    Err(error @ NvFwUpdError::AuthFailed { .. }) => last_error = error,
                    Err(error) => last_error = unreachable_error(error.to_string()),
                }
            }
            Ok(state) => last_error = unreachable_error(format!("system power state: {state:?}")),
            Err(error @ (RedfishError::Unauthenticated(_) | RedfishError::Forbidden(_))) => {
                last_error = redfish_error(error);
            }
            Err(error) => last_error = unreachable_error(error.to_string()),
        }
        if attempt < super::SWITCH_ACTIVATION_RECOVERY_ATTEMPTS {
            cancellable(
                &cancel,
                tokio::time::sleep(Duration::from_secs(
                    super::SWITCH_ACTIVATION_RECOVERY_INTERVAL_SECS,
                )),
            )
            .await?;
        }
    }
    Err(last_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redfish_client::BmcCredentials;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn client(server: &MockServer) -> RedfishClient {
        // `RedfishClient::new` shares one process-wide HTTP connection pool. Each test binds a
        // fresh mock server on an ephemeral port, and a reused port can inherit a dead pooled
        // connection from an earlier test, which surfaces as a spurious transport error under
        // load. Non-default timeouts give this client its own isolated HTTP client.
        RedfishClient::new_with_timeouts(
            "127.0.0.1",
            server.address().port(),
            BmcCredentials::token("bmc-token".to_owned()),
            false,
            false,
            Duration::from_secs(5),
            Duration::from_secs(20),
        )
        .unwrap()
    }

    async fn mount_json(server: &MockServer, uri: &str, body: Value) {
        Mock::given(method("GET"))
            .and(path(uri))
            .and(header("X-Auth-Token", "bmc-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(server)
            .await;
    }

    async fn mount_inventory(server: &MockServer) {
        mount_json(
            server,
            INVENTORY_URI,
            json!({"Members":[{"@odata.id": format!("{INVENTORY_URI}/MGX_FW_BMC_0")}]}),
        )
        .await;
        mount_json(
            server,
            &format!("{INVENTORY_URI}/MGX_FW_BMC_0"),
            json!({"Id":"MGX_FW_BMC_0", "Version":"88.0060.0451", "Updateable":true}),
        )
        .await;
    }

    async fn mount_system(server: &MockServer, power_state: &str) {
        mount_json(
            server,
            "/redfish/v1",
            json!({"@odata.id":"/redfish/v1", "Id":"RootService", "Name":"Root Service", "Links":{},
            "Systems":{"@odata.id":"/redfish/v1/Systems"}}),
        )
        .await;
        mount_json(
            server,
            "/redfish/v1/Systems",
            json!({"@odata.id":"/redfish/v1/Systems",
            "@odata.type":"#ComputerSystemCollection.ComputerSystemCollection", "Name":"Systems",
            "Members":[{"@odata.id":"/redfish/v1/Systems/System_0"}]}),
        )
        .await;
        mount_json(
            server,
            "/redfish/v1/Systems/System_0",
            json!({"@odata.id":"/redfish/v1/Systems/System_0",
            "Id":"System_0", "Name":"Switch", "PowerState":power_state,
            "Actions":{"#ComputerSystem.Reset":{"target":"/redfish/v1/Oem/ResetSwitch"}}}),
        )
        .await;
    }

    const RECOVERY_URIS: [&str; 3] = [
        "/redfish/v1/Systems/System_0",
        INVENTORY_URI,
        "/redfish/v1/UpdateService/FirmwareInventory/MGX_FW_BMC_0",
    ];

    async fn mount_recovery_sequence(server: &MockServer, uri: &'static str, statuses: &[u16]) {
        mount_system(server, "On").await;
        mount_inventory(server).await;
        let reset_sent = Arc::new(AtomicBool::new(false));
        let trigger = reset_sent.clone();
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Oem/ResetSwitch"))
            .and(body_json(json!({"ResetType":"PowerCycle"})))
            .respond_with(move |_: &wiremock::Request| {
                trigger.store(true, Ordering::SeqCst);
                ResponseTemplate::new(204)
            })
            .expect(1)
            .mount(server)
            .await;
        let body = match uri {
            "/redfish/v1/Systems/System_0" => json!({
                "@odata.id": uri, "Id": "System_0", "Name": "Switch", "PowerState": "On"
            }),
            INVENTORY_URI => json!({"Members":[{"@odata.id": RECOVERY_URIS[2]}]}),
            _ => json!({"Id":"MGX_FW_BMC_0", "Version":"88.0060.0451"}),
        };
        // A failed direct power-state read is also tried through collection discovery.
        let statuses: Vec<_> = statuses
            .iter()
            .flat_map(|&status| {
                std::iter::repeat_n(
                    status,
                    if uri == RECOVERY_URIS[0] && status != 200 {
                        2
                    } else {
                        1
                    },
                )
            })
            .collect();
        let count = statuses.len() as u64;
        let attempt = AtomicUsize::new(0);
        Mock::given(method("GET"))
            .and(path(uri))
            .and(header("X-Auth-Token", "bmc-token"))
            .and(move |_: &wiremock::Request| reset_sent.load(Ordering::SeqCst))
            .respond_with(move |_: &wiremock::Request| {
                let index = attempt.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(statuses.get(index).copied().unwrap_or(500))
                    .set_body_json(&body)
            })
            .with_priority(1)
            .expect(count)
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn inventory_follows_pages_and_preserves_every_bmc_instance() {
        let server = MockServer::start().await;
        mount_json(
            &server,
            &format!("{INVENTORY_URI}/MGX_FW_BMC_0"),
            json!({"Id":"MGX_FW_BMC_0","Version":"88.0060.0451"}),
        )
        .await;
        mount_json(
            &server,
            INVENTORY_URI,
            json!({
                "Members":[{"@odata.id":format!("{INVENTORY_URI}/MGX_FW_BMC_0")}],
                "Members@odata.nextLink":"/redfish/v1/Oem/InventoryPage2"
            }),
        )
        .await;
        mount_json(
            &server,
            "/redfish/v1/Oem/InventoryPage2",
            json!({
                "Members":[{"@odata.id":format!("{INVENTORY_URI}/MGX_FW_ERoT_BMC_0")}]
            }),
        )
        .await;
        mount_json(
            &server,
            &format!("{INVENTORY_URI}/MGX_FW_ERoT_BMC_0"),
            json!({"Id":"MGX_FW_ERoT_BMC_0","Version":"2.0"}),
        )
        .await;
        let inventory = firmware_inventory(&client(&server)).await.unwrap();
        assert_eq!(inventory.len(), 2);
        assert_eq!(
            inventory[&format!("{INVENTORY_URI}/MGX_FW_BMC_0")]["Version"],
            "88.0060.0451"
        );
        assert_eq!(
            inventory[&format!("{INVENTORY_URI}/MGX_FW_ERoT_BMC_0")]["Version"],
            "2.0"
        );
        assert!(server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| r.url.path().starts_with("/redfish/")));
    }

    #[tokio::test]
    async fn inventory_rejects_member_failures_instead_of_returning_partial_results() {
        for status in [401, 500] {
            let server = MockServer::start().await;
            mount_json(
                &server,
                &format!("{INVENTORY_URI}/MGX_FW_BMC_0"),
                json!({"Id":"MGX_FW_BMC_0","Version":"88.0060.0451"}),
            )
            .await;
            mount_json(
                &server,
                INVENTORY_URI,
                json!({"Members":[
                    {"@odata.id":format!("{INVENTORY_URI}/MGX_FW_BMC_0")},
                    {"@odata.id":format!("{INVENTORY_URI}/MGX_FW_ERoT_BMC_0")}
                ]}),
            )
            .await;
            Mock::given(method("GET"))
                .and(path(format!("{INVENTORY_URI}/MGX_FW_ERoT_BMC_0")))
                .respond_with(ResponseTemplate::new(status))
                .mount(&server)
                .await;
            let error = firmware_inventory(&client(&server)).await.unwrap_err();
            if status == 401 {
                assert!(matches!(error, NvFwUpdError::AuthFailed { .. }));
            }
        }
    }

    #[tokio::test]
    async fn inventory_rejects_invalid_empty_cyclic_and_cross_origin_collections() {
        let other = MockServer::start().await;
        for collection in [
            json!({}),
            json!({"Members":[]}),
            json!({"Members":[{}]}),
            json!({"Members":[], "Members@odata.nextLink":INVENTORY_URI}),
            json!({"Members":[{"@odata.id":format!("{}/secret", other.uri())}]}),
        ] {
            let server = MockServer::start().await;
            mount_json(&server, INVENTORY_URI, collection).await;
            assert!(firmware_inventory(&client(&server)).await.is_err());
        }
        assert!(other.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn activation_resets_once_and_checks_bmc_recovery_without_nvue() {
        let server = MockServer::start().await;
        mount_system(&server, "On").await;
        mount_inventory(&server).await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Oem/ResetSwitch"))
            .and(header("X-Auth-Token", "bmc-token"))
            .and(body_json(json!({"ResetType":"PowerCycle"})))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        let summary = activate(&client(&server), None).await.unwrap();
        assert_eq!(summary.details["command"], "BMC_REDFISH_POWER_CYCLE");
        assert!(server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| r.url.path().starts_with("/redfish/")));
    }

    #[tokio::test]
    async fn activation_recovery_is_bounded_and_does_not_reset_again() {
        let server = MockServer::start().await;
        mount_system(&server, "Off").await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Oem/ResetSwitch"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        assert!(matches!(
            activate(&client(&server), None).await,
            Err(NvFwUpdError::BmcUnreachable { .. })
        ));
    }

    #[tokio::test]
    async fn activation_retries_transient_authentication_errors() {
        for uri in RECOVERY_URIS {
            for status in [401, 403] {
                let server = MockServer::start().await;
                mount_recovery_sequence(&server, uri, &[status, 200]).await;
                let summary = activate(&client(&server), None).await.unwrap();
                assert_eq!(summary.details["recovery"]["attempts"], 2);
                server.verify().await;
            }
        }
    }

    #[tokio::test]
    async fn activation_preserves_last_authentication_error_when_budget_expires() {
        for uri in RECOVERY_URIS {
            for statuses in [
                [401, 401, 401],
                [403, 403, 403],
                [401, 500, 403],
                [403, 500, 401],
            ] {
                let server = MockServer::start().await;
                mount_recovery_sequence(&server, uri, &statuses).await;
                let error = activate(&client(&server), None).await.unwrap_err();
                let NvFwUpdError::AuthFailed { target, message } = error else {
                    panic!("expected final authentication error: {error:?}");
                };
                assert_eq!(target, "switch BMC");
                assert!(
                    message.contains(&format!("HTTP {}", statuses[2])),
                    "{message}"
                );
                server.verify().await;
            }
        }
    }

    #[tokio::test]
    async fn activation_reports_later_non_authentication_failure() {
        for uri in RECOVERY_URIS {
            for status in [401, 403] {
                let server = MockServer::start().await;
                mount_recovery_sequence(&server, uri, &[status, 500, 500]).await;
                let error = activate(&client(&server), None).await.unwrap_err();
                let NvFwUpdError::BmcUnreachable { message, .. } = error else {
                    panic!("expected final recovery error: {error:?}");
                };
                assert!(message.contains("HTTP 500"), "{message}");
                server.verify().await;
            }
        }
    }

    #[tokio::test]
    async fn activation_reset_authentication_failure_is_not_retried() {
        for status in [401, 403] {
            let server = MockServer::start().await;
            mount_system(&server, "On").await;
            Mock::given(method("POST"))
                .and(path("/redfish/v1/Oem/ResetSwitch"))
                .respond_with(ResponseTemplate::new(status))
                .expect(1)
                .mount(&server)
                .await;
            assert!(matches!(
                activate(&client(&server), None).await,
                Err(NvFwUpdError::AuthFailed { .. })
            ));
            let requests = server.received_requests().await.unwrap();
            assert_eq!(
                requests
                    .iter()
                    .filter(|request| request.url.path() == RECOVERY_URIS[0])
                    .count(),
                1,
                "recovery must not start when the reset is rejected"
            );
            assert!(!requests
                .iter()
                .any(|request| request.url.path() == INVENTORY_URI));
            server.verify().await;
        }
    }

    #[tokio::test]
    async fn activation_cancellation_during_authentication_recovery() {
        let server = MockServer::start().await;
        mount_system(&server, "On").await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Oem/ResetSwitch"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        Mock::given(method("GET"))
            .and(path(INVENTORY_URI))
            .respond_with(move |_: &wiremock::Request| {
                trigger.cancel();
                ResponseTemplate::new(401)
            })
            .expect(1)
            .mount(&server)
            .await;
        assert!(matches!(
            activate(&client(&server), Some(cancel)).await,
            Err(NvFwUpdError::Cancelled { .. })
        ));
        server.verify().await;
    }

    #[tokio::test]
    async fn activation_cancellation_before_reset_and_during_recovery() {
        let server = MockServer::start().await;
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(matches!(
            activate(&client(&server), Some(cancel)).await,
            Err(NvFwUpdError::Cancelled { .. })
        ));
        assert!(server.received_requests().await.unwrap().is_empty());

        mount_system(&server, "Off").await;
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        Mock::given(method("POST"))
            .and(path("/redfish/v1/Oem/ResetSwitch"))
            .respond_with(move |_: &wiremock::Request| {
                trigger.cancel();
                ResponseTemplate::new(204)
            })
            .expect(1)
            .mount(&server)
            .await;
        assert!(matches!(
            activate(&client(&server), Some(cancel)).await,
            Err(NvFwUpdError::Cancelled { .. })
        ));
    }
}
