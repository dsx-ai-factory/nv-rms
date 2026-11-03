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

//! ACP and FNM administrative link configuration through NVUE.

use std::collections::BTreeMap;

use super::SwitchGb200Nvidia;
use super::nvue::RevisionApplyOutcome;
use crate::transport::http_client::HttpClient;
use crate::utilities::error::{Result, RmsError};

use nvue_client::interface::{
    INTERFACE_APPLIED_CONFIG_ENDPOINT, INTERFACE_APPLIED_ENDPOINT, INTERFACE_ENDPOINT,
    INTERFACE_STARTUP_CONFIG_ENDPOINT, InterfaceCollection, InterfaceConfig, InterfaceLinkState,
    interface_link_patch,
};

use serde::Deserialize;

/// One expanded ACP or FNM interface state change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScaleUpLinkChange {
    /// Expanded interface name, not a range or wildcard expression.
    pub(crate) interface_id: String,

    /// Explicit desired administrative state.
    pub(crate) state: InterfaceLinkState,
}

/// The root configuration omits interfaces without explicit configuration.
#[derive(Deserialize)]
struct InterfaceRootConfig {
    #[serde(default)]
    interface: InterfaceCollection,
}

impl SwitchGb200Nvidia {
    /// Reconciles ACP and FNM link state in one applied and saved revision.
    ///
    /// Matching applied and startup overrides avoid writes. A saved-state-only
    /// difference saves the applied configuration without creating a revision.
    /// Callers serialize mutations with the switch operation lock and verify
    /// effective state separately; operational link health is not changed here.
    ///
    /// # Errors
    ///
    /// Rejects a batch containing an invalid interface ID before device I/O.
    pub(crate) async fn set_scale_up_link_config(
        &self,
        changes: &[ScaleUpLinkChange],
    ) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }

        if let Some(change) = changes
            .iter()
            .find(|change| !is_scale_up_interface(&change.interface_id))
        {
            return Err(RmsError::invalid_argument(format!(
                "invalid scale-up interface ID '{}'",
                change.interface_id
            )));
        }

        let current = self.get_applied_scale_up_link_config().await?;

        let patch: BTreeMap<String, InterfaceConfig> = changes
            .iter()
            .filter(|change| !link_state_matches(&current, change))
            .map(|change| {
                (
                    change.interface_id.clone(),
                    interface_link_patch(change.state),
                )
            })
            .collect();

        if patch.is_empty() {
            let startup = self.get_startup_scale_up_link_config().await?;

            if changes
                .iter()
                .all(|change| link_state_matches(&startup, change))
            {
                return Ok(());
            }

            // A prior attempt may have applied the desired state but failed to
            // save it. Persist the applied configuration without creating a
            // redundant revision.
            return self.save_current_applied_config().await;
        }

        let payload = serde_json::to_value(patch).map_err(|error| {
            RmsError::internal(format!(
                "failed to serialize interface link configuration: {error}"
            ))
        })?;

        let outcome = self
            .nvue_apply_config_patches_with_outcome(&[(INTERFACE_ENDPOINT, payload)])
            .await?;

        if outcome == RevisionApplyOutcome::NoConfigDiff {
            // The desired state may have appeared after the initial read. Save
            // the now-current applied state so the successful request remains
            // persistent across a switch restart.
            self.save_current_applied_config().await?;
        }

        Ok(())
    }

    /// Reads all ACP and FNM administrative link states reported by NVUE.
    pub(crate) async fn get_scale_up_link_config(&self) -> Result<InterfaceCollection> {
        self.read_scale_up_link_config(INTERFACE_APPLIED_ENDPOINT)
            .await
    }

    /// Reads explicit applied overrides; missing entries remain absent rather than default UP.
    pub(crate) async fn get_applied_scale_up_link_config(&self) -> Result<InterfaceCollection> {
        self.read_scale_up_link_config(INTERFACE_APPLIED_CONFIG_ENDPOINT)
            .await
    }

    /// Reads persisted startup overrides independently of the current effective state.
    pub(crate) async fn get_startup_scale_up_link_config(&self) -> Result<InterfaceCollection> {
        self.read_scale_up_link_config(INTERFACE_STARTUP_CONFIG_ENDPOINT)
            .await
    }

    async fn read_scale_up_link_config(&self, endpoint: &str) -> Result<InterfaceCollection> {
        let response = self
            .nvue_http_get(endpoint, HttpClient::DEFAULT_TIMEOUT)
            .await?;

        if !response.is_object() {
            return Err(RmsError::internal(
                "invalid NVUE interface response: expected an object",
            ));
        }

        // The interface endpoint fills defaults even with filled=false. Only
        // root configuration reads distinguish absent overrides from explicit UP.
        let interfaces: InterfaceCollection = if endpoint == INTERFACE_APPLIED_ENDPOINT {
            serde_json::from_value(response)
        } else {
            serde_json::from_value::<InterfaceRootConfig>(response).map(|root| root.interface)
        }
        .map_err(|error| RmsError::internal(format!("invalid NVUE interface response: {error}")))?;

        Ok(interfaces
            .into_iter()
            .filter(|(interface_id, _)| is_scale_up_interface(interface_id))
            .collect())
    }
}

fn link_state_matches(current: &InterfaceCollection, desired: &ScaleUpLinkChange) -> bool {
    let current = current.get(&desired.interface_id);
    current.and_then(|interface| interface.link.administrative_state()) == Some(desired.state)
}

fn is_scale_up_interface(interface_id: &str) -> bool {
    interface_number(interface_id, "acp").is_some()
        || interface_number(interface_id, "fnm").is_some()
}

/// Returns a numeric suffix only when the whole interface ID matches.
pub(crate) fn interface_number(interface_id: &str, family: &str) -> Option<u32> {
    let suffix = interface_id.strip_prefix(family)?;

    if suffix.is_empty() || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    suffix.parse().ok().filter(|index| *index > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nvue_client::revision::{RevisionCreate, RevisionUpdate};
    use wiremock::matchers::{body_json, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn interface_filter_accepts_only_numbered_acp_and_fnm_interfaces() {
        assert!(is_scale_up_interface("acp72"));
        assert!(is_scale_up_interface("fnm2"));
        assert!(!is_scale_up_interface("acp0"));
        assert!(!is_scale_up_interface("fnm0"));
        assert!(!is_scale_up_interface("acp"));
        assert!(!is_scale_up_interface("acp1/../../system"));
        assert!(!is_scale_up_interface("eth0"));
    }

    #[tokio::test]
    async fn link_configuration_rejects_invalid_batches_before_device_io() {
        let server = MockServer::start().await;
        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        for interface_id in [
            "eth0",
            "acp0",
            "fnm0",
            "acp",
            "fnm",
            "acp1-8",
            "acp1,2",
            "acp1/../../system",
        ] {
            let changes = [
                ScaleUpLinkChange {
                    interface_id: "acp1".to_owned(),
                    state: InterfaceLinkState::Down,
                },
                ScaleUpLinkChange {
                    interface_id: interface_id.to_owned(),
                    state: InterfaceLinkState::Down,
                },
            ];

            let error = switch.set_scale_up_link_config(&changes).await.unwrap_err();

            assert_eq!(
                error.code,
                crate::utilities::error::ErrorCode::InvalidArgument
            );

            assert!(error.message.contains(interface_id));
        }

        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn link_configuration_applies_changed_interfaces_once() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/nvue_v1/"))
            .and(query_param("rev", "applied"))
            .and(query_param("filled", "false"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "interface": {
                    "acp1": {"link": {"state": {"up": {}}}},
                    "acp2": {"link": {"state": {"down": {}}}},
                    "eth0": {"link": {"state": {"up": {}}}},
                    "fnm1": {"link": {"state": {"down": {}}}}
                }
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/nvue_v1/revision"))
            .and(body_json(RevisionCreate::default()))
            .respond_with(ResponseTemplate::new(200).set_body_json("42"))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("PATCH"))
            .and(path(INTERFACE_ENDPOINT))
            .and(query_param("rev", "42"))
            .and(body_json(serde_json::json!({
                "acp2": {"link": {"state": {"up": {}}}},
                "fnm1": {"link": {"state": {"up": {}}}}
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("PATCH"))
            .and(path("/nvue_v1/revision/42"))
            .and(body_json(RevisionUpdate::apply_with_yes_prompt()))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/revision/42"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "42": {"state": "applied_and_saved"}
            })))
            .expect(1)
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());
        switch
            .set_scale_up_link_config(&[
                ScaleUpLinkChange {
                    interface_id: "acp1".to_owned(),
                    state: InterfaceLinkState::Up,
                },
                ScaleUpLinkChange {
                    interface_id: "acp2".to_owned(),
                    state: InterfaceLinkState::Up,
                },
                ScaleUpLinkChange {
                    interface_id: "fnm1".to_owned(),
                    state: InterfaceLinkState::Up,
                },
            ])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn no_config_diff_is_saved_after_the_initial_applied_read() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/"))
            .and(query_param("rev", "applied"))
            .and(query_param("filled", "false"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "interface": {"acp1": {"link": {"state": {"down": {}}}}}
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("POST"))
            .and(path("/nvue_v1/revision"))
            .and(body_json(RevisionCreate::default()))
            .respond_with(ResponseTemplate::new(200).set_body_json("42"))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("PATCH"))
            .and(path(INTERFACE_ENDPOINT))
            .and(query_param("rev", "42"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("PATCH"))
            .and(path("/nvue_v1/revision/42"))
            .and(body_json(RevisionUpdate::apply_with_yes_prompt()))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/revision/42"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "42": {
                    "state": "invalid",
                    "transition": {
                        "issue": {
                            "issue-00000": {
                                "message": "config apply executed with no config diff",
                                "severity": "info"
                            }
                        }
                    }
                }
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("PATCH"))
            .and(path("/nvue_v1/revision/applied"))
            .and(body_json(RevisionUpdate::save_with_yes_prompt()))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());
        switch
            .set_scale_up_link_config(&[ScaleUpLinkChange {
                interface_id: "acp1".to_owned(),
                state: InterfaceLinkState::Up,
            }])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn matching_applied_and_startup_links_require_no_writes() {
        let server = MockServer::start().await;

        for revision in ["applied", "startup"] {
            Mock::given(method("GET"))
                .and(path("/nvue_v1/"))
                .and(query_param("rev", revision))
                .and(query_param("filled", "false"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "interface": {"acp1": {"link": {"state": {"up": {}}}}}
                })))
                .expect(1)
                .mount(&server)
                .await;
        }

        SwitchGb200Nvidia::for_test(&server.uri())
            .set_scale_up_link_config(&[ScaleUpLinkChange {
                interface_id: "acp1".to_owned(),
                state: InterfaceLinkState::Up,
            }])
            .await
            .unwrap();

        let requests = server.received_requests().await.unwrap();

        assert_eq!(requests.len(), 2);

        assert!(
            requests
                .iter()
                .all(|request| request.method == wiremock::http::Method::Get)
        );
    }

    #[tokio::test]
    async fn applied_state_is_saved_when_startup_state_differs() {
        let server = MockServer::start().await;

        for (revision, state) in [("applied", "up"), ("startup", "down")] {
            Mock::given(method("GET"))
                .and(path("/nvue_v1/"))
                .and(query_param("rev", revision))
                .and(query_param("filled", "false"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "interface": {"acp1": {"link": {"state": {state: {}}}}}
                })))
                .expect(1)
                .mount(&server)
                .await;
        }

        Mock::given(method("PATCH"))
            .and(path("/nvue_v1/revision/applied"))
            .and(body_json(RevisionUpdate::save_with_yes_prompt()))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        switch
            .set_scale_up_link_config(&[ScaleUpLinkChange {
                interface_id: "acp1".to_owned(),
                state: InterfaceLinkState::Up,
            }])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn saved_link_reads_preserve_explicit_states_without_inherited_defaults() {
        let server = MockServer::start().await;

        for revision in ["applied", "startup"] {
            Mock::given(method("GET"))
                .and(path("/nvue_v1/"))
                .and(query_param("rev", revision))
                .and(query_param("filled", "false"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "header": {},
                    "interface": {
                        "acp1": {"link": {"state": {"up": {}}}},
                        "acp2": {"link": {"state": {"down": {}}}},
                        "fnm1": {"type": "fnm"},
                        "eth0": {"link": {"state": {"up": {}}}}
                    }
                })))
                .expect(1)
                .mount(&server)
                .await;
        }

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        let applied = switch.get_applied_scale_up_link_config().await.unwrap();
        let startup = switch.get_startup_scale_up_link_config().await.unwrap();

        for interfaces in [applied, startup] {
            assert_eq!(interfaces.len(), 3);

            assert_eq!(
                interfaces["acp1"].link.administrative_state(),
                Some(InterfaceLinkState::Up)
            );

            assert_eq!(
                interfaces["acp2"].link.administrative_state(),
                Some(InterfaceLinkState::Down)
            );

            assert!(interfaces["fnm1"].link.state.is_none());
        }
    }

    #[tokio::test]
    async fn saved_link_reads_reject_malformed_root_configuration_and_read_failures() {
        for response in [
            ResponseTemplate::new(500),
            ResponseTemplate::new(200).set_body_json(serde_json::json!(null)),
            ResponseTemplate::new(200).set_body_json(serde_json::json!([])),
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"interface": null})),
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"interface": []})),
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "interface": {"acp1": {"link": "invalid"}}
            })),
        ] {
            let server = MockServer::start().await;

            Mock::given(method("GET"))
                .and(path("/nvue_v1/"))
                .and(query_param("rev", "applied"))
                .and(query_param("filled", "false"))
                .respond_with(response)
                .expect(1)
                .mount(&server)
                .await;

            let switch = SwitchGb200Nvidia::for_test(&server.uri());

            assert!(switch.get_applied_scale_up_link_config().await.is_err());
        }
    }

    #[tokio::test]
    async fn link_configuration_reads_filled_applied_state() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path(INTERFACE_ENDPOINT))
            .and(query_param("rev", "applied"))
            .and(query_param("filled", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "acp1": {"link": {"state": {"up": {}}}},
                "eth0": {"link": {"state": {"up": {}}}},
                "fnm1": {"link": {"state": {"down": {}}}}
            })))
            .expect(1)
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        let interfaces = switch.get_scale_up_link_config().await.unwrap();

        assert_eq!(interfaces.len(), 2);

        assert_eq!(
            interfaces["acp1"].link.administrative_state(),
            Some(InterfaceLinkState::Up)
        );

        assert_eq!(
            interfaces["fnm1"].link.administrative_state(),
            Some(InterfaceLinkState::Down)
        );
    }
}
