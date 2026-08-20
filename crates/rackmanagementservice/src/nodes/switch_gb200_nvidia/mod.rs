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

//! NVIDIA GB200 switch node implementation.
//!
//! Provides node construction and management workflows over NVUE, SSH/SFTP,
//! Redfish, and NVFWUPD.

mod cluster;
mod factory_reset;
mod firmware;
mod nvue;
mod password;
mod power;
mod ssh;
mod system_image;
mod validation;

pub mod config;
pub mod mtls;

pub(crate) use self::password::SwitchSystemPasswordUpdateOutcome;
pub(crate) use self::validation::{is_valid_identifier, shell_quote};

pub use self::cluster::grpc_port_for_app;
pub use self::system_image::{
    NVOS_PARTITION_1_ID, NVOS_PARTITION_2_ID, SwitchSystemImageState, describe_install_poll,
    infer_target_build_id, is_http_unauthorized_error, is_install_handoff_to_reboot,
    is_install_poll_success, is_install_progress_state, is_install_reboot_transition_state,
    is_retryable_install_poll_error, is_retryable_nvue_transport_error,
    is_retryable_steady_state_error, is_uninstall_poll_success, normalize_system_image_state,
    system_image_listing_contains,
};

use std::collections::HashMap;
#[cfg(test)]
use std::sync::Arc;

use async_trait::async_trait;
use nvue_client::SharedClient as SharedNvueClient;
#[cfg(test)]
use nvue_client::{
    Client as NvueClient, ClientConfig as NvueConnectConfig, ClientCredentials as NvueCredentials,
    ClientEndpoint as NvueEndpoint,
};
use secrecy::ExposeSecret;

#[cfg(test)]
use redfish_client::BmcCredentials;

#[cfg(test)]
use self::cluster::SDN_FACTORY_RESET_UNCONFIGURED_CONFIRMATION_POLLS;
#[cfg(test)]
use self::firmware::{
    FwpkgCpldSubcomponent, build_cpld_package_version, classify_firmware_type_switch,
    contains_reboot_hint, extract_action_issue_message, extract_switch_version_string,
    indicates_noop, map_package_device_name, normalize_cpld_version_string, strip_leading_zeros,
};
use self::nvue::{build_nvue_api, build_nvue_connect_config, extract_job_id};
use self::power::switch_redfish_reset_type;
#[cfg(test)]
use self::validation::{
    firmware_inventory_endpoint_component, is_valid_component, is_valid_firmware_filename,
    is_valid_firmware_inventory_component, is_valid_system_username,
};
use crate::domain::node::*;
use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials, NodeConfig};
use crate::nodes::nvfwupd_adapter;
use crate::transport::http_client::HttpClient;
use crate::transport::redfish_client::{
    RedfishClient, ResetType as RedfishResetType, redfish_client_from_endpoint_config,
};
#[cfg(test)]
use crate::utilities::error::ErrorCode;
use crate::utilities::error::{Result, RmsError};

#[cfg(test)]
type SshExecForTest = dyn Fn(&str) -> Result<String> + Send + Sync;

#[cfg(test)]
type SshPasswordRecoveryForTest = dyn Fn(&str, &str) -> Result<()> + Send + Sync;

// ═══════════════════════════════════════════════════════════════════
//  NVIDIA GB200 switch node (NVUE REST + SSH/SFTP)
// ═══════════════════════════════════════════════════════════════════

/// Concrete node for NVIDIA reference GB200 network switches (NVUE REST + SSH/SFTP).
///
/// Power:     Redfish ComputerSystem.Reset when BMC details are available;
///            legacy PowerCycle fallback via POST /nvue_v1/system
/// Firmware:  SFTP push then NVUE install, poll via /nvue_v1/action/{id}
/// Activate:  power cycle
///
/// Credentials are stored at construction time. One shared NVUE client is
/// kept for REST calls; SshClient instances are created on demand
/// for each SFTP transfer or SSH command.
pub struct SwitchGb200Nvidia {
    id: String,
    rack_id: String,
    node_type: NodeType,
    bmc_endpoint: Option<EndpointConfig>,
    host_endpoint: Option<EndpointConfig>,
    expected_inventory: Option<ExpectedInventoryPolicy>,
    nvue: Option<SharedNvueClient>,
    redfish: Option<RedfishClient>,

    // Serializes full node operations across shared NVUE, Redfish, and
    // NVFWUPD clients. The guard intentionally spans awaited protocol calls.
    pub(crate) op_lock: tokio::sync::Mutex<()>,

    #[cfg(test)]
    pub(crate) ssh_exec_for_test: Option<Arc<SshExecForTest>>,

    #[cfg(test)]
    ssh_password_recovery_for_test: Option<Arc<SshPasswordRecoveryForTest>>,
}

fn redfish_from_endpoint(endpoint: &EndpointConfig) -> Result<Option<RedfishClient>> {
    let Some(credentials) = endpoint.credentials.as_ref() else {
        return Ok(None);
    };

    if endpoint.endpoint.ip_address.is_empty()
        || endpoint.endpoint.port == 0
        || credentials.username.is_empty()
        || credentials.password.expose_secret().is_empty()
    {
        return Ok(None);
    }

    redfish_client_from_endpoint_config(endpoint).map(Some)
}

impl SwitchGb200Nvidia {
    pub const TYPE: &'static str = "switch_gb200_nvidia";

    /// Node identifier (inherent accessor — use this instead of the private `id` field).
    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    pub(crate) fn rack_id(&self) -> &str {
        &self.rack_id
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        rack_id: String,
        host: String,
        port: u16,
        username: &str,
        password: &str,
        mac_address: String,
        host_mac_address: String,
        host_ip_address: String,
        host_name: String,
        dangerously_accept_invalid_certs: bool,
    ) -> Result<Self> {
        if host_ip_address.is_empty() {
            return Err(RmsError::invalid_argument(
                "host_ip_address is required for switch nodes",
            ));
        }

        let host_name = if host_name.is_empty() {
            None
        } else {
            Some(host_name)
        };
        let host_endpoint = EndpointConfig::with_credentials(
            Endpoint {
                ip_address: host_ip_address,
                mac_address: host_mac_address,
                port,
                host_name,
            },
            Some(EndpointCredentials::new(username, password)),
            dangerously_accept_invalid_certs,
        );
        let nvue_config = build_nvue_connect_config(&host_endpoint, username, password)?;
        let nvue = build_nvue_api(nvue_config)?;

        Ok(Self {
            id,
            rack_id,
            node_type: NodeType::SwitchGb200Nvidia,
            bmc_endpoint: Some(EndpointConfig::new(Endpoint {
                ip_address: host,
                mac_address,
                port,
                host_name: None,
            })),
            host_endpoint: Some(host_endpoint),
            expected_inventory: None,
            nvue: Some(nvue),
            redfish: None,
            op_lock: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            ssh_exec_for_test: None,
            #[cfg(test)]
            ssh_password_recovery_for_test: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_redfish_power(
        id: String,
        rack_id: String,
        bmc_host: String,
        bmc_port: u16,
        username: &str,
        password: &str,
        mac_address: String,
        dangerously_accept_invalid_certs: bool,
    ) -> Result<Self> {
        let bmc_endpoint = EndpointConfig::with_credentials(
            Endpoint {
                ip_address: bmc_host,
                mac_address,
                port: bmc_port,
                host_name: None,
            },
            Some(EndpointCredentials::new(username, password)),
            dangerously_accept_invalid_certs,
        );

        let redfish = Some(redfish_client_from_endpoint_config(&bmc_endpoint)?);

        Ok(Self {
            id,
            rack_id,
            node_type: NodeType::SwitchGb200Nvidia,
            redfish,
            bmc_endpoint: Some(bmc_endpoint),
            host_endpoint: None,
            expected_inventory: None,
            nvue: None,
            op_lock: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            ssh_exec_for_test: None,
            #[cfg(test)]
            ssh_password_recovery_for_test: None,
        })
    }

    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        if config.node_type.kind() != NodeKind::Switch {
            return Err(RmsError::unimplemented(
                "create_node",
                config.node_type.as_str(),
            ));
        }

        // Host/NVUE traffic must use host_endpoint. BMC-only switches can still
        // be created for BMC Redfish power, but they must not bind the NVUE REST
        // client to the BMC endpoint because that sends host operations to the
        // wrong management plane.
        let nvue = if let Some(host_endpoint) = config.host_endpoint.as_ref() {
            if host_endpoint.endpoint.ip_address.is_empty() {
                return Err(RmsError::invalid_argument(
                    "host_ip_address is required for switch nodes",
                ));
            }

            let http_credentials = host_endpoint.credentials.as_ref();
            let username = http_credentials
                .map(|credentials| credentials.username.as_str())
                .unwrap_or_default();
            let password = http_credentials
                .map(|credentials| credentials.password.expose_secret())
                .unwrap_or_default();
            let nvue_config = build_nvue_connect_config(host_endpoint, username, password)?;
            Some(build_nvue_api(nvue_config)?)
        } else {
            let Some(bmc_endpoint) = config.bmc_endpoint.as_ref() else {
                return Err(RmsError::invalid_argument(
                    "bmc_endpoint is required when host_endpoint is not set for switch nodes",
                ));
            };

            if bmc_endpoint.endpoint.ip_address.is_empty() {
                return Err(RmsError::invalid_argument(
                    "bmc_ip_address is required for BMC-only switch nodes",
                ));
            }

            if bmc_endpoint.endpoint.mac_address.is_empty() {
                return Err(RmsError::invalid_argument(
                    "bmc_mac_address is required for BMC-only switch nodes",
                ));
            }

            None
        };

        // Power traffic is separate from host/NVUE traffic. When valid BMC
        // details and BMC credentials exist, create a Redfish client with the
        // BMC endpoint auth. This prevents host/NVUE credentials from being
        // accidentally reused for BMC power when both endpoints are present.
        let redfish = config
            .bmc_endpoint
            .as_ref()
            .map(redfish_from_endpoint)
            .transpose()?
            .flatten();

        Ok(Self {
            id: config.id.clone(),
            rack_id: rack_id.to_owned(),
            node_type: config.node_type,
            bmc_endpoint: config.bmc_endpoint.clone(),
            host_endpoint: config.host_endpoint.clone(),
            expected_inventory: config.expected_inventory.clone(),
            nvue,
            redfish,
            op_lock: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            ssh_exec_for_test: None,
            #[cfg(test)]
            ssh_password_recovery_for_test: None,
        })
    }

    fn require_host_endpoint(&self) -> Result<&Endpoint> {
        self.host_endpoint
            .as_ref()
            .map(|endpoint| &endpoint.endpoint)
            .ok_or_else(|| {
                RmsError::failed_precondition(
                    "switch host endpoint is required for NVUE/NVOS operations",
                )
            })
    }

    fn require_host_credentials(&self) -> Result<&EndpointCredentials> {
        self.host_endpoint
            .as_ref()
            .and_then(|endpoint| endpoint.credentials.as_ref())
            .ok_or_else(|| {
                RmsError::failed_precondition(
                    "switch host credentials are required for NVUE/NVOS operations",
                )
            })
    }

    pub(crate) fn host_credentials(&self) -> Result<(&str, &str)> {
        let credentials = self.require_host_credentials()?;
        Ok((&credentials.username, credentials.password.expose_secret()))
    }
}

// ── Node trait implementation ───────────────────────────────────────

#[async_trait]
impl Node for SwitchGb200Nvidia {
    fn id(&self) -> &str {
        SwitchGb200Nvidia::id(self)
    }

    fn rack_id(&self) -> &str {
        SwitchGb200Nvidia::rack_id(self)
    }

    fn node_type(&self) -> NodeType {
        self.node_type
    }

    fn get_info(&self) -> HashMap<String, String> {
        let mut info = HashMap::from([("type".to_owned(), self.node_type.as_str().to_owned())]);
        if let Some(endpoint) = &self.bmc_endpoint {
            info.insert("host".to_owned(), endpoint.endpoint.ip_address.clone());
            info.insert("port".to_owned(), endpoint.endpoint.port.to_string());
            info.insert(
                "macAddress".to_owned(),
                endpoint.endpoint.mac_address.clone(),
            );
        }

        if let Some(endpoint) = &self.host_endpoint {
            if !endpoint.endpoint.mac_address.is_empty() {
                info.insert(
                    "hostMac_0".to_owned(),
                    endpoint.endpoint.mac_address.clone(),
                );
            }

            info.insert("hostIp_0".to_owned(), endpoint.endpoint.ip_address.clone());
        }

        info
    }

    fn expected_inventory_policy(&self) -> Option<&ExpectedInventoryPolicy> {
        self.expected_inventory.as_ref()
    }

    async fn get_power_state(&self) -> Result<PowerState> {
        let _op_guard = self.op_lock.lock().await;

        if let Some(redfish) = &self.redfish {
            tracing::debug!(node = %self.id, "get_power_state via BMC Redfish");

            return match redfish.computer_system_power_state("System_0").await {
                Ok(state) => Ok(state.into()),
                Err(error) => {
                    tracing::warn!(
                        node = %self.id,
                        error = %error,
                        "get_power_state: BMC Redfish status unavailable; returning Unknown"
                    );

                    Ok(PowerState::Unknown)
                }
            };
        }

        let host = self
            .host_endpoint
            .as_ref()
            .map(|endpoint| endpoint.endpoint.ip_address.as_str())
            .unwrap_or_default();

        tracing::debug!(node = %self.id, host, "get_power_state via NVOS fallback");
        match self
            .nvue_http_get("/nvue_v1/system", HttpClient::DEFAULT_TIMEOUT)
            .await
        {
            Ok(_) => Ok(PowerState::On),
            Err(e) => {
                tracing::warn!(
                    node = %self.id,
                    host,
                    error_code = ?e.code,
                    error_msg = %e.message,
                    "get_power_state: NVOS status unavailable; returning Unknown"
                );
                Ok(PowerState::Unknown)
            }
        }
    }

    async fn set_power_state(&self, op: PowerOp, _target: PowerTargetType) -> Result<()> {
        let _op_guard = self.op_lock.lock().await;
        tracing::info!(node = %self.id, ?op, "set_power_state");

        if let Some(redfish) = &self.redfish {
            let reset_type = switch_redfish_reset_type(op)?;

            redfish
                .reset_computer_system("System_0", reset_type)
                .await?;

            return Ok(());
        }

        if op != PowerOp::PowerCycle {
            return Err(RmsError::invalid_argument(
                "switch nodes require BMC Redfish power details for non-PowerCycle operations",
            ));
        }

        let payload = serde_json::json!({
            "@power-cycle": {
                "state": "start",
                "parameters": {"force": true}
            }
        });
        self.nvue_http_post("/nvue_v1/system", &payload, HttpClient::DEFAULT_TIMEOUT)
            .await?;
        Ok(())
    }

    async fn get_firmware_inventory(&self) -> Result<Vec<FirmwareInfo>> {
        let _op_guard = self.op_lock.lock().await;
        let host_endpoint = self.require_host_endpoint()?;
        tracing::debug!(node = %self.id, host = %host_endpoint.ip_address, "get_firmware_inventory via NVFWUPD");

        let inventory = self
            .nvfwupd_workflow()?
            .get_firmware_inventory(self.nvfwupd_target_config()?)
            .await
            .map_err(nvfwupd_adapter::map_error)?;

        Ok(inventory
            .into_iter()
            .map(nvfwupd_adapter::to_firmware_info)
            .collect())
    }

    async fn verify_firmware_package_versions(
        &self,
        targets: &[FirmwareTarget],
    ) -> Result<FirmwareVersionCheckSummary> {
        let _op_guard = self.op_lock.lock().await;
        let host_endpoint = self.require_host_endpoint()?;
        tracing::info!(
            node = %self.id,
            host = %host_endpoint.ip_address,
            "verify_firmware_package_versions via NVFWUPD"
        );
        let targets = nvfwupd_adapter::to_version_check_targets(targets);
        let summary = self
            .nvfwupd_workflow()?
            .verify_firmware_target_versions(self.nvfwupd_target_config()?, targets)
            .await
            .map_err(nvfwupd_adapter::map_error)?;
        Ok(nvfwupd_adapter::map_version_check_summary(summary))
    }

    async fn update_firmware(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
        options: FirmwareUpdateOptions,
    ) -> Result<FirmwareUpdateOutcome> {
        let _op_guard = self.op_lock.lock().await;
        let host_endpoint = self.require_host_endpoint()?;

        tracing::info!(
            node = %self.id,
            host = %host_endpoint.ip_address,
            component = %target.component,
            file = %target.firmware_file,
            force_update,
            "update_firmware via NVFWUPD"
        );
        let request = nvfwupd_adapter::to_update_request(target, force_update, options);
        let expected_inventory = self
            .expected_inventory_policy()
            .map(|policy| policy.ap_names.as_ref().to_vec());
        let outcome = self
            .nvfwupd_workflow()?
            .update_firmware_with_expected_inventory(
                self.nvfwupd_target_config()?,
                request,
                expected_inventory,
            )
            .await
            .map_err(nvfwupd_adapter::map_error)?;

        Ok(nvfwupd_adapter::map_outcome(outcome))
    }

    async fn poll_firmware_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
        let _op_guard = self.op_lock.lock().await;
        tracing::debug!(node = %self.id, task_id, "poll_firmware_task via legacy NVUE poller");
        self.poll_nvue_action_task(task_id).await
    }

    async fn activate_firmware_with(
        &self,
        request: FirmwareActivationRequest,
    ) -> Result<FirmwareActivationSummary> {
        let _op_guard = self.op_lock.lock().await;
        tracing::info!(node = %self.id, "activate_firmware via NVFWUPD");
        let request = nvfwupd_adapter::to_activation_request(request);
        let summary = self
            .nvfwupd_workflow()?
            .activate_firmware(self.nvfwupd_target_config()?, request)
            .await
            .map_err(nvfwupd_adapter::map_error)?;

        Ok(nvfwupd_adapter::map_activation_summary(summary))
    }

    async fn activate_firmware(&self) -> Result<()> {
        self.activate_firmware_with(FirmwareActivationRequest {
            mode: FirmwareActivationMode::SwitchPowerCycle,
            cancellation: None,
        })
        .await
        .map(|_| ())
    }

    /// Returns true if a Redfish client for the BMC is configured.
    fn supports_bmc_aux_powercycle(&self) -> bool {
        self.redfish.is_some()
    }

    /// Power-cycles the switch host (NVOS) through the BMC using the Redfish
    /// `ComputerSystem.Reset` action.  This is used as a fallback when the
    /// switch host is unreachable via the NVOS API and a hard reset is needed
    /// to recover it.
    ///
    /// Requires that a BMC Redfish endpoint is configured for this node.
    async fn bmc_aux_powercycle(&self) -> Result<()> {
        let redfish = self.redfish.as_ref().ok_or_else(|| {
            RmsError::failed_precondition(
                "BMC endpoint is required for aux powercycle on switch nodes",
            )
        })?;

        let _op_guard = self.op_lock.lock().await;
        tracing::info!(node = %self.id, "bmc_aux_powercycle via nv-redfish ComputerSystem.Reset");
        redfish
            .reset_computer_system("System_0", RedfishResetType::PowerCycle)
            .await?;

        Ok(())
    }
}

// ═══════════════════════════════════════════════════════════════════
//  Tests
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
impl SwitchGb200Nvidia {
    /// Test-only constructor that creates a Switch instance with a mock HTTP client.
    /// Public so it can be used by other test modules (i.e. switch_image_handlers.rs).
    ///
    /// Note: `host_endpoint.endpoint.ip_address` is set to the full mock URL,
    /// so this constructor only supports testing HTTP-based methods. SSH/SFTP
    /// operations will fail.
    pub fn for_test(base_url: &str) -> Self {
        let host_endpoint = EndpointConfig::with_credentials(
            Endpoint {
                ip_address: base_url.to_owned(),
                mac_address: "00:00:00:00:00:00".to_owned(),
                port: 443,
                host_name: Some("test-host".to_owned()),
            },
            Some(EndpointCredentials::new("test-username", "test-password")),
            true,
        );

        let url = reqwest::Url::parse(base_url).expect("test NVUE URL");

        let config = NvueConnectConfig {
            endpoint: NvueEndpoint::http(
                url.host_str().expect("test NVUE host"),
                url.port_or_known_default().expect("test NVUE port"),
            ),
            credentials: NvueCredentials::new("test-username", "test-password"),
            dangerously_accept_invalid_certs: true,
        };

        let nvue = NvueClient::new(config).expect("test NVUE client");
        Self {
            id: "test-switch".to_owned(),
            rack_id: "test-rack".to_owned(),
            node_type: NodeType::SwitchGb200Nvidia,
            bmc_endpoint: Some(EndpointConfig::new(Endpoint {
                ip_address: "test-host".to_owned(),
                mac_address: "00:00:00:00:00:00".to_owned(),
                port: 443,
                host_name: None,
            })),
            host_endpoint: Some(host_endpoint),
            expected_inventory: None,
            nvue: Some(nvue),
            redfish: None,
            op_lock: tokio::sync::Mutex::new(()),
            #[cfg(test)]
            ssh_exec_for_test: None,
            #[cfg(test)]
            ssh_password_recovery_for_test: None,
        }
    }

    /// Replace test host credentials and rebuild the embedded NVUE mock client.
    pub fn with_host_credentials_for_test(mut self, username: &str, password: &str) -> Self {
        let host_endpoint = self
            .host_endpoint
            .as_mut()
            .expect("test switch has host endpoint");

        host_endpoint.credentials = Some(EndpointCredentials::new(username, password));

        let url = reqwest::Url::parse(&host_endpoint.endpoint.ip_address).expect("test NVUE URL");

        let config = NvueConnectConfig {
            endpoint: NvueEndpoint::http(
                url.host_str().expect("test NVUE host"),
                url.port_or_known_default().expect("test NVUE port"),
            ),
            credentials: NvueCredentials::new(username, password),
            dangerously_accept_invalid_certs: true,
        };

        self.nvue = Some(NvueClient::new(config).expect("test NVUE client"));
        self
    }

    /// Clears the BMC endpoint so the node represents a switch that has no BMC
    /// endpoint configured. Use this in tests that exercise the "no BMC
    /// endpoint" code paths (e.g. `bmc_aux_powercycle` returning
    /// `FailedPrecondition`).
    pub fn without_bmc_endpoint_for_test(mut self) -> Self {
        self.bmc_endpoint = None;
        self.redfish = None;
        self
    }

    pub fn with_redfish_for_test(mut self, base_url: &str) -> Self {
        self.bmc_endpoint = Some(EndpointConfig::with_credentials(
            Endpoint {
                ip_address: base_url.to_owned(),
                mac_address: "00:00:00:00:00:00".to_owned(),
                port: 443,
                host_name: None,
            },
            Some(EndpointCredentials::new(
                "test-bmc-username",
                "test-bmc-password",
            )),
            true,
        ));

        let url = reqwest::Url::parse(base_url).expect("test BMC URL");

        self.redfish = Some(
            RedfishClient::new(
                url.host_str().expect("test BMC host"),
                url.port_or_known_default().expect("test BMC port"),
                BmcCredentials::username_password(
                    "test-bmc-username".to_owned(),
                    Some("test-bmc-password".to_owned()),
                ),
                true,
                url.scheme() == "https",
            )
            .expect("test Redfish client"),
        );

        self
    }

    pub fn with_ssh_exec_for_test(
        mut self,
        exec: impl Fn(&str) -> Result<String> + Send + Sync + 'static,
    ) -> Self {
        self.ssh_exec_for_test = Some(Arc::new(exec));
        self
    }

    pub fn with_ssh_password_recovery_for_test(
        mut self,
        recover: impl Fn(&str, &str) -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        self.ssh_password_recovery_for_test = Some(Arc::new(recover));
        self
    }
}

#[cfg(test)]
mod tests;
