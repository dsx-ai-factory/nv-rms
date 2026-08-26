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

use crate::domain::node::{NodeType as DomainNodeType, PowerOp, PowerState};
use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials};
use crate::orchestrator::job_lifecycle::{JobError, JobState};
use crate::orchestrator::job_tracker::JobInfo;
use crate::utilities::error::{Result as RmsResult, RmsError};
use chrono::{DateTime, Utc};
use librms::protos::rack_manager as pb;
use secrecy::{ExposeSecret, SecretString};
use thiserror::Error;

// ── Power conversions ──

/// Error returned when a protobuf power operation cannot map to a domain operation.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum PowerOperationConversionError {
    /// The caller left the operation at its default UNSPECIFIED value.
    #[error("power operation is unspecified")]
    Unspecified,
}

impl TryFrom<pb::PowerOperation> for PowerOp {
    type Error = PowerOperationConversionError;

    fn try_from(op: pb::PowerOperation) -> Result<Self, Self::Error> {
        match op {
            // UNSPECIFIED is a valid protobuf value, but not a domain power
            // operation. Treating it as Err is the required-field validation.
            pb::PowerOperation::Unspecified => Err(PowerOperationConversionError::Unspecified),
            pb::PowerOperation::On => Ok(Self::On),
            pb::PowerOperation::Off => Ok(Self::Off),
            pb::PowerOperation::Reset => Ok(Self::PowerCycle),
            pb::PowerOperation::ForceOn => Ok(Self::ForceOn),
            pb::PowerOperation::ForceOff => Ok(Self::ForceOff),
            pb::PowerOperation::GracefulShutdown => Ok(Self::GracefulShutdown),
            pb::PowerOperation::GracefulRestart => Ok(Self::GracefulRestart),
            pb::PowerOperation::ForceRestart => Ok(Self::ForceRestart),
        }
    }
}

impl TryFrom<pb::RackPowerOperation> for PowerOp {
    type Error = PowerOperationConversionError;

    fn try_from(op: pb::RackPowerOperation) -> Result<Self, Self::Error> {
        match op {
            // UNSPECIFIED is a valid protobuf value, but not a domain power
            // operation. Treating it as Err is the required-field validation.
            pb::RackPowerOperation::Unspecified => Err(PowerOperationConversionError::Unspecified),
            pb::RackPowerOperation::On => Ok(Self::On),
            pb::RackPowerOperation::Off => Ok(Self::Off),
            pb::RackPowerOperation::Cycle => Ok(Self::PowerCycle),
        }
    }
}

pub fn power_state_to_string(state: PowerState) -> &'static str {
    match state {
        PowerState::On => "ON",
        PowerState::Off => "OFF",
        PowerState::Unknown => "UNKNOWN",
    }
}

// ── Node type conversions ──

pub fn proto_node_type_to_domain(nt: i32) -> Option<DomainNodeType> {
    match pb::NodeType::try_from(nt).ok()? {
        pb::NodeType::ComputeGb200Nvidia => Some(DomainNodeType::ComputeGb200Nvidia),
        pb::NodeType::PowershelfGb200Liteon => Some(DomainNodeType::PowershelfGb200Liteon),
        pb::NodeType::SwitchGb200Nvidia => Some(DomainNodeType::SwitchGb200Nvidia),
        pb::NodeType::PowershelfGb200Delta => Some(DomainNodeType::PowershelfGb200Delta),
        pb::NodeType::ComputeGb300Nvidia => Some(DomainNodeType::ComputeGb300Nvidia),
        pb::NodeType::SwitchGb300Nvidia => Some(DomainNodeType::SwitchGb300Nvidia),
        pb::NodeType::PowershelfGb300Liteon => Some(DomainNodeType::PowershelfGb300Liteon),
        pb::NodeType::PowershelfGb300Delta => Some(DomainNodeType::PowershelfGb300Delta),
        pb::NodeType::ComputeGb300Lenovo => Some(DomainNodeType::ComputeGb300Lenovo),
        pb::NodeType::ComputeVrnvl72Nvidia => Some(DomainNodeType::ComputeVrnvl72Nvidia),
        pb::NodeType::SwitchVrnvl72Nvidia => Some(DomainNodeType::SwitchVrnvl72Nvidia),
        pb::NodeType::Unspecified => None,
    }
}

pub fn domain_node_type_to_proto(node_type: DomainNodeType) -> pb::NodeType {
    match node_type {
        DomainNodeType::ComputeGb200Nvidia => pb::NodeType::ComputeGb200Nvidia,
        // Wiwynn GB200 is descriptor-only on the wire. Returning Unspecified
        // keeps the descriptor authoritative without extending the protobuf enum.
        DomainNodeType::ComputeGb200Wiwynn => pb::NodeType::Unspecified,
        DomainNodeType::PowershelfGb200Liteon => pb::NodeType::PowershelfGb200Liteon,
        DomainNodeType::SwitchGb200Nvidia => pb::NodeType::SwitchGb200Nvidia,
        DomainNodeType::PowershelfGb200Delta => pb::NodeType::PowershelfGb200Delta,
        DomainNodeType::ComputeGb300Nvidia => pb::NodeType::ComputeGb300Nvidia,
        DomainNodeType::SwitchGb300Nvidia => pb::NodeType::SwitchGb300Nvidia,
        DomainNodeType::PowershelfGb300Liteon => pb::NodeType::PowershelfGb300Liteon,
        DomainNodeType::PowershelfGb300Delta => pb::NodeType::PowershelfGb300Delta,
        DomainNodeType::ComputeGb300Lenovo => pb::NodeType::ComputeGb300Lenovo,
        // Supermicro GB300 is descriptor-only on the wire. Returning
        // Unspecified keeps the descriptor authoritative without adding a
        // protobuf enum value.
        DomainNodeType::ComputeGb300Supermicro => pb::NodeType::Unspecified,
        DomainNodeType::ComputeVrnvl72Nvidia => pb::NodeType::ComputeVrnvl72Nvidia,
        DomainNodeType::SwitchVrnvl72Nvidia => pb::NodeType::SwitchVrnvl72Nvidia,
    }
}

pub fn proto_node_type_to_string(nt: i32) -> Option<&'static str> {
    match pb::NodeType::try_from(nt).ok()? {
        pb::NodeType::Unspecified => None,
        supported => proto_node_type_to_domain(supported as i32).map(DomainNodeType::as_str),
    }
}

// ── NodeInfo / NodeUpdateInfo flattening ──

/// Flattened endpoint values used by legacy `NodeConfig` construction.
#[derive(Debug, Clone, Default)]
pub struct FlatEndpointConfig {
    pub ip_address: String,
    pub mac_address: String,
    pub host_name: Option<String>,
    pub port: u32,
    pub username: String,
    pub password: SecretString,
    pub dangerously_accept_invalid_certs: bool,
}

impl PartialEq for FlatEndpointConfig {
    fn eq(&self, other: &Self) -> bool {
        self.ip_address == other.ip_address
            && self.mac_address == other.mac_address
            && self.host_name == other.host_name
            && self.port == other.port
            && self.username == other.username
            && self.password.expose_secret() == other.password.expose_secret()
            && self.dangerously_accept_invalid_certs == other.dangerously_accept_invalid_certs
    }
}

impl Eq for FlatEndpointConfig {}

// Flattened view of a NodeInfo's endpoint fields. The BMC and host endpoint
// configs stay separate so switch NVUE/NVOS calls do not accidentally inherit
// BMC-only connection settings.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlatEndpoint {
    pub bmc: Option<FlatEndpointConfig>,
    pub host: Option<FlatEndpointConfig>,
}

impl FlatEndpoint {
    /// Return the optional BMC endpoint when the request provided one.
    ///
    /// Missing BMC is valid for host-only switch calls. A present BMC endpoint
    /// requires an IP address, but its MAC address is optional because switch
    /// BMC Redfish operations do not use it.
    pub fn optional_bmc_endpoint(&self) -> RmsResult<Option<EndpointConfig>> {
        let Some(endpoint) = self.bmc.as_ref() else {
            return Ok(None);
        };

        Self::endpoint_from_config("BMC", endpoint, false).map(Some)
    }

    /// Return the required BMC endpoint.
    pub fn bmc_endpoint(&self) -> RmsResult<EndpointConfig> {
        let Some(endpoint) = self.bmc.as_ref() else {
            return Err(RmsError::invalid_argument("missing BMC endpoint"));
        };

        Self::endpoint_from_config("BMC", endpoint, true)
    }

    /// Return the required host endpoint.
    ///
    /// Host MAC is optional for direct switch requests that only need host IP,
    /// port, and credentials.
    pub fn host_endpoint(&self) -> RmsResult<EndpointConfig> {
        let Some(endpoint) = self.host.as_ref() else {
            return Err(RmsError::invalid_argument("missing host endpoint"));
        };

        Self::endpoint_from_config("host", endpoint, false)
    }

    /// Return the optional host endpoint when the request provided one.
    ///
    /// Host MAC is optional for direct switch requests that only need host IP,
    /// port, and credentials.
    pub fn optional_host_endpoint(&self) -> RmsResult<Option<EndpointConfig>> {
        let Some(endpoint) = self.host.as_ref() else {
            return Ok(None);
        };

        Self::endpoint_from_config("host", endpoint, false).map(Some)
    }

    /// Return the required switch host endpoint for NVUE/HTTPS management.
    ///
    /// Switch CSVs can use one port field for both NVUE and SSH. A host port of
    /// 22 means SSH for upload, but RMS/NVUE calls should still use the default
    /// HTTPS port. Secure switch traffic validates the switch server
    /// certificate; request fields cannot disable this policy.
    pub fn switch_host_management_endpoint(&self) -> RmsResult<EndpointConfig> {
        let mut endpoint = self.host_endpoint()?;
        endpoint.endpoint.port = Self::switch_management_port(endpoint.endpoint.port);
        Ok(endpoint)
    }

    /// Return the optional switch host endpoint with NVUE/HTTPS port handling.
    pub fn optional_switch_host_management_endpoint(&self) -> RmsResult<Option<EndpointConfig>> {
        if self.host.is_none() {
            return Ok(None);
        }

        self.switch_host_management_endpoint().map(Some)
    }

    /// Return the credentials the given node type should use.
    ///
    /// Switches authenticate to NVUE/NVOS via their host endpoint; everyone
    /// else talks Redfish on the BMC. Returns `None` when the selected endpoint
    /// or either credential field is missing.
    pub fn creds_for_node_type(&self, node_type: DomainNodeType) -> Option<(&str, &str)> {
        let endpoint = if node_type.uses_host_management_endpoint() {
            self.host.as_ref()
        } else {
            self.bmc.as_ref()
        }?;

        if endpoint.username.is_empty() || endpoint.password.expose_secret().is_empty() {
            return None;
        }

        Some((
            endpoint.username.as_str(),
            endpoint.password.expose_secret(),
        ))
    }

    /// Return the TLS validation override for the endpoint used by a node type.
    pub fn dangerously_accept_invalid_certs_for_node_type(
        &self,
        node_type: DomainNodeType,
    ) -> bool {
        let endpoint = if node_type.uses_host_management_endpoint() {
            self.host.as_ref()
        } else {
            self.bmc.as_ref()
        };

        endpoint
            .map(|e| e.dangerously_accept_invalid_certs)
            .unwrap_or(false)
    }

    /// Resolve the endpoint port used by the given node type.
    pub fn resolve_port_for_node_type(&self, node_type: DomainNodeType) -> RmsResult<u16> {
        let (endpoint_name, endpoint) = if node_type.uses_host_management_endpoint() {
            ("host endpoint for switch", self.host.as_ref())
        } else {
            ("BMC endpoint", self.bmc.as_ref())
        };

        let Some(endpoint) = endpoint else {
            return Err(RmsError::invalid_argument(format!(
                "missing {endpoint_name}"
            )));
        };

        Self::resolve_port(endpoint.port)
    }

    /// Resolve the BMC port for an ephemeral node.
    pub fn resolve_bmc_port(&self) -> RmsResult<u16> {
        let Some(endpoint) = self.bmc.as_ref() else {
            return Err(RmsError::invalid_argument("missing BMC endpoint"));
        };

        Self::resolve_port(endpoint.port)
    }

    /// Return the host endpoint IP when the request provided one.
    pub fn host_ip_address(&self) -> Option<&str> {
        self.host
            .as_ref()
            .map(|e| e.ip_address.as_str())
            .filter(|ip| !ip.is_empty())
    }

    /// Return the host endpoint MAC when the request provided one.
    pub fn host_mac_address(&self) -> Option<&str> {
        self.host
            .as_ref()
            .map(|e| e.mac_address.as_str())
            .filter(|mac| !mac.is_empty())
    }

    /// Return the switch host name used for TLS connections to the host endpoint.
    /// Falls back to `ip_address` when `host_name` is not set.
    pub fn switch_host_name(&self) -> RmsResult<&str> {
        let Some(host) = self.host.as_ref() else {
            return Err(RmsError::invalid_argument("missing host endpoint"));
        };
        if let Some(name) = host.host_name.as_deref().filter(|name| !name.is_empty()) {
            return Ok(name);
        }
        if host.ip_address.is_empty() {
            return Err(RmsError::invalid_argument(
                "missing host endpoint ip_address",
            ));
        }
        Ok(host.ip_address.as_str())
    }

    /// Resolve an endpoint port. The proto field is `u32`,
    /// but `NodeConfig::port` is `u16`, so we need an explicit range check to
    /// avoid silently truncating (e.g. a caller-supplied `65537` would wrap to
    /// `1`). Returns `443` when the caller did not supply a value (`0`),
    /// otherwise the port if it fits in `u16`, otherwise a descriptive error.
    fn resolve_port(port: u32) -> RmsResult<u16> {
        if port == 0 {
            return Ok(443);
        }

        u16::try_from(port).map_err(|_| {
            RmsError::invalid_argument(format!("port {port} is out of range (must be 1-65535)"))
        })
    }

    fn switch_management_port(port: u16) -> u16 {
        if port == 22 { 443 } else { port }
    }

    fn endpoint_from_config(
        name: &str,
        endpoint: &FlatEndpointConfig,
        require_mac_address: bool,
    ) -> RmsResult<EndpointConfig> {
        // Name the specific missing field so a per-node failure is actionable
        // rather than an opaque "missing fields". Requiredness is
        // context-dependent (see `flat_endpoint_config`): ip is always
        // required here, mac only when `require_mac_address` is set.
        if endpoint.ip_address.is_empty() {
            return Err(RmsError::invalid_argument(format!(
                "missing {name} endpoint ip_address"
            )));
        }
        if require_mac_address && endpoint.mac_address.is_empty() {
            return Err(RmsError::invalid_argument(format!(
                "missing {name} endpoint mac_address"
            )));
        }

        let credentials =
            if endpoint.username.is_empty() || endpoint.password.expose_secret().is_empty() {
                None
            } else {
                Some(EndpointCredentials::new(
                    endpoint.username.clone(),
                    endpoint.password.expose_secret().to_owned(),
                ))
            };

        Ok(EndpointConfig::with_credentials(
            Endpoint {
                ip_address: endpoint.ip_address.clone(),
                mac_address: endpoint.mac_address.clone(),
                port: Self::resolve_port(endpoint.port)?,
                host_name: endpoint.host_name.clone(),
            },
            credentials,
            endpoint.dangerously_accept_invalid_certs,
        ))
    }

    /// Resolve the port RMS should store in `NodeConfig` for management API
    /// calls. Switch CSVs historically use one port column for both NVUE/HTTPS
    /// and SSH. NVFWUPD's switch upload path opens SSH on port 22 itself, so a
    /// switch port of 22 here means "use the default NVUE port" for RMS.
    pub fn resolve_management_port_for_node_type(
        &self,
        node_type: DomainNodeType,
    ) -> RmsResult<u16> {
        let port = self.resolve_port_for_node_type(node_type)?;
        if node_type.uses_host_management_endpoint() && port == 22 {
            Ok(443)
        } else {
            Ok(port)
        }
    }
}

fn extract_user_pass(
    endpoint_name: &str,
    credentials: Option<&pb::Credentials>,
) -> RmsResult<(String, SecretString)> {
    let Some(auth) = credentials.and_then(|c| c.auth.as_ref()) else {
        return Ok(Default::default());
    };

    match auth {
        pb::credentials::Auth::UserPass(up) => {
            Ok((up.username.clone(), SecretString::from(up.password.clone())))
        }
        pb::credentials::Auth::SessionToken(_) => Err(RmsError::invalid_argument(format!(
            "{endpoint_name} endpoint credentials use unsupported session_token auth; use user_pass credentials"
        ))),
    }
}

fn flat_endpoint_config(
    endpoint_name: &str,
    endpoint: &pb::Endpoint,
) -> RmsResult<FlatEndpointConfig> {
    let iface = endpoint.interface.as_ref();
    let (username, password) = extract_user_pass(endpoint_name, endpoint.credentials.as_ref())?;

    Ok(FlatEndpointConfig {
        // Absent IP/MAC default to empty here and are validated later by
        // `endpoint_from_config` / `switch_host_name`. Validation is
        // intentionally deferred to those accessors -- not done at this
        // boundary -- because requiredness is context-dependent: a BMC
        // endpoint always needs an ip, a host endpoint is optional at
        // registration, and a MAC is required only for some roles. This
        // flattening step has no node-kind/role context to decide that.
        ip_address: iface.map(|i| i.ip_address.clone()).unwrap_or_default(),
        mac_address: iface.map(|i| i.mac_address.clone()).unwrap_or_default(),
        host_name: iface
            .and_then(|i| i.host_name.clone())
            .filter(|name| !name.is_empty()),
        port: endpoint.port,
        username,
        password,
        // TLS validation policy is owned by RMS for each management target.
        dangerously_accept_invalid_certs: false,
    })
}

/// Flatten endpoint fields from a protobuf `NodeInfo`.
///
/// # Errors
///
/// Returns an `InvalidArgument` error when an endpoint uses an unsupported
/// credential auth type.
pub fn flatten_node_info(ni: &pb::NodeInfo) -> RmsResult<FlatEndpoint> {
    Ok(FlatEndpoint {
        bmc: ni
            .bmc_endpoint
            .as_ref()
            .map(|endpoint| flat_endpoint_config("BMC", endpoint))
            .transpose()?,
        host: ni
            .host_endpoint
            .as_ref()
            .map(|endpoint| flat_endpoint_config("host", endpoint))
            .transpose()?,
    })
}

impl From<JobState> for pb::FirmwareJobState {
    fn from(state: JobState) -> Self {
        match state {
            JobState::Queued => Self::Queued,
            JobState::Running => Self::Running,
            JobState::Completed => Self::Completed,
            JobState::Failed => Self::Failed,
        }
    }
}

impl From<JobState> for pb::JobExecutionState {
    fn from(state: JobState) -> Self {
        match state {
            JobState::Queued => Self::Queued,
            JobState::Running => Self::Running,
            JobState::Completed => Self::Completed,
            JobState::Failed => Self::Failed,
        }
    }
}

// Firmware-specific async status still uses its legacy error enum. Keep this
// mapping lossy for now; eventually firmware and similar async task APIs should
// be able to report the generic `JobError` directly.
impl From<JobError> for pb::FirmwareUpdateError {
    fn from(err: JobError) -> Self {
        match err {
            JobError::Unspecified => Self::Success,
            JobError::Internal => Self::Exception,
            JobError::Other => Self::TaskFailed,
            JobError::InvalidArgument => Self::ClientFailure,
            // The legacy firmware enum has no failed-precondition value.
            JobError::FailedPrecondition => Self::ClientFailure,
            JobError::ClientError => Self::ClientFailure,
            JobError::ServerError => Self::ServerError,
            JobError::Timeout => Self::MonitoringTimeout,
            JobError::InvalidResponse => Self::InvalidResponse,
            JobError::Unauthenticated => Self::ClientFailure,
            JobError::TargetNotFound => Self::TargetNotFound,
            JobError::FileNotFound => Self::FileNotFound,
            JobError::UpdateInProgress => Self::UpdateInProgress,
        }
    }
}

impl From<JobError> for pb::JobError {
    fn from(err: JobError) -> Self {
        match err {
            JobError::Unspecified => Self::Unspecified,
            JobError::Internal => Self::Internal,
            JobError::Other => Self::Other,
            JobError::InvalidArgument => Self::InvalidArgument,
            // The generic protobuf enum predates this domain classification.
            JobError::FailedPrecondition => Self::ClientError,
            JobError::ClientError => Self::ClientError,
            JobError::ServerError => Self::ServerError,
            JobError::Timeout => Self::Timeout,
            JobError::InvalidResponse => Self::InvalidResponse,
            JobError::Unauthenticated => Self::Unauthenticated,
            JobError::TargetNotFound => Self::TargetNotFound,
            JobError::FileNotFound => Self::FileNotFound,
            JobError::UpdateInProgress => Self::UpdateInProgress,
        }
    }
}

impl From<JobInfo> for pb::JobStatus {
    fn from(info: JobInfo) -> Self {
        Self {
            job_id: info.job_id,
            parent_job_id: info.parent_job_id,
            child_job_ids: info.child_job_ids,
            execution_state: pb::JobExecutionState::from(info.state).into(),
            error_message: info.error_message,
            error_code: pb::JobError::from(info.error_code).into(),
            result_json: info.result_json,
            state_description: info.state_description,
            rack_id: Some(info.rack_id),
            node_id: Some(info.node_id),
            created_at: Some(timestamp_from_datetime(info.created_at)),
            updated_at: Some(timestamp_from_datetime(info.updated_at)),
        }
    }
}

/// Converts an absolute UTC timestamp into a protobuf `Timestamp`.
pub fn timestamp_from_datetime(datetime: DateTime<Utc>) -> prost_types::Timestamp {
    prost_types::Timestamp {
        seconds: datetime.timestamp(),
        nanos: datetime.timestamp_subsec_nanos() as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::utilities::error::ErrorCode;

    #[test]
    fn job_info_converts_to_job_status() {
        let created_at = DateTime::from_timestamp(1_000, 123).unwrap();
        let updated_at = DateTime::from_timestamp(2_000, 456).unwrap();
        let status = pb::JobStatus::from(JobInfo {
            job_id: "child-job".to_owned(),
            span: tracing::Span::none(),
            state: JobState::Failed,
            state_description: "Update failed".to_owned(),
            rack_id: "rack-1".to_owned(),
            node_id: "node-1".to_owned(),
            error_code: JobError::Timeout,
            error_message: "Timed out".to_owned(),
            result_json: r#"{"status":"failed"}"#.to_owned(),
            created_at,
            updated_at,
            child_job_ids: Vec::new(),
            parent_job_id: Some("parent-job".to_owned()),
        });

        assert_eq!(status.job_id, "child-job");
        assert_eq!(status.parent_job_id.as_deref(), Some("parent-job"));
        assert_eq!(status.execution_state, pb::JobExecutionState::Failed as i32);
        assert_eq!(status.error_code, pb::JobError::Timeout as i32);
        assert_eq!(status.rack_id.as_deref(), Some("rack-1"));
        assert_eq!(status.node_id.as_deref(), Some("node-1"));
        assert_eq!(status.created_at.unwrap().seconds, 1_000);
        assert_eq!(status.updated_at.unwrap().seconds, 2_000);
    }

    // ── flatten_node_info credential source ────────────────────────

    fn user_pass_creds(u: &str, p: &str) -> pb::Credentials {
        pb::Credentials {
            auth: Some(pb::credentials::Auth::UserPass(pb::UsernamePassword {
                username: u.into(),
                password: p.into(),
            })),
        }
    }

    #[test]
    fn flatten_populates_host_creds_when_present() -> RmsResult<()> {
        let ni = pb::NodeInfo {
            node_id: "sw-01".into(),
            rack_id: "r".into(),
            r#type: Some(pb::NodeType::SwitchGb200Nvidia as i32),
            bmc_endpoint: Some(pb::Endpoint {
                interface: Some(pb::NetworkInterface {
                    ip_address: "10.0.0.1".into(),
                    mac_address: "aa:bb:cc:00:00:11".into(),
                    host_name: None,
                }),
                port: 443,
                credentials: Some(user_pass_creds("bmc_user", "bmc_pass")),
            }),
            host_endpoint: Some(pb::Endpoint {
                interface: Some(pb::NetworkInterface {
                    ip_address: "10.0.1.1".into(),
                    mac_address: "aa:bb:cc:00:01:11".into(),
                    host_name: None,
                }),
                port: 22,
                credentials: Some(user_pass_creds("host_user", "host_pass")),
            }),
            ..Default::default()
        };

        let flat = flatten_node_info(&ni)?;

        assert_eq!(
            flat.bmc.as_ref().map(|e| e.username.as_str()),
            Some("bmc_user")
        );
        assert_eq!(
            flat.bmc.as_ref().map(|e| e.password.expose_secret()),
            Some("bmc_pass")
        );
        assert_eq!(
            flat.host.as_ref().map(|e| e.username.as_str()),
            Some("host_user")
        );
        assert_eq!(
            flat.host.as_ref().map(|e| e.password.expose_secret()),
            Some("host_pass")
        );
        assert_eq!(
            flat.bmc
                .as_ref()
                .map(|e| e.dangerously_accept_invalid_certs),
            Some(false)
        );
        assert_eq!(
            flat.host
                .as_ref()
                .map(|e| e.dangerously_accept_invalid_certs),
            Some(false)
        );

        Ok(())
    }

    #[test]
    fn flatten_populates_host_name_from_proto() -> RmsResult<()> {
        let ni = pb::NodeInfo {
            node_id: "sw-01".into(),
            rack_id: "r".into(),
            r#type: Some(pb::NodeType::SwitchGb200Nvidia as i32),
            bmc_endpoint: None,
            host_endpoint: Some(pb::Endpoint {
                interface: Some(pb::NetworkInterface {
                    ip_address: "10.0.1.1".into(),
                    mac_address: "aa:bb:cc:00:01:11".into(),
                    host_name: Some("switch.example.com".into()),
                }),
                port: 443,
                credentials: Some(user_pass_creds("host_user", "host_pass")),
            }),
            ..Default::default()
        };

        let flat = flatten_node_info(&ni)?;
        assert_eq!(
            flat.host.as_ref().and_then(|e| e.host_name.as_deref()),
            Some("switch.example.com")
        );
        assert_eq!(flat.switch_host_name()?, "switch.example.com");

        Ok(())
    }

    #[test]
    fn flatten_rejects_host_session_token_credentials() {
        let ni = pb::NodeInfo {
            node_id: "sw-01".into(),
            rack_id: "r".into(),
            r#type: Some(pb::NodeType::SwitchGb200Nvidia as i32),
            bmc_endpoint: None,
            host_endpoint: Some(pb::Endpoint {
                interface: Some(pb::NetworkInterface {
                    ip_address: "10.0.1.1".into(),
                    mac_address: "aa:bb:cc:00:01:11".into(),
                    host_name: None,
                }),
                port: 22,
                credentials: Some(pb::Credentials {
                    auth: Some(pb::credentials::Auth::SessionToken("token".into())),
                }),
            }),
            ..Default::default()
        };

        let result = flatten_node_info(&ni);
        let Err(err) = result else {
            panic!("expected session_token credentials to be rejected");
        };

        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("host endpoint credentials"));
        assert!(err.message.contains("session_token"));
        assert!(err.message.contains("user_pass"));
    }

    #[test]
    fn flatten_rejects_bmc_session_token_credentials() {
        let ni = pb::NodeInfo {
            node_id: "cn-01".into(),
            rack_id: "r".into(),
            r#type: Some(pb::NodeType::ComputeGb200Nvidia as i32),
            bmc_endpoint: Some(pb::Endpoint {
                interface: Some(pb::NetworkInterface {
                    ip_address: "10.0.0.1".into(),
                    mac_address: "aa:bb:cc:00:00:11".into(),
                    host_name: None,
                }),
                port: 443,
                credentials: Some(pb::Credentials {
                    auth: Some(pb::credentials::Auth::SessionToken("token".into())),
                }),
            }),
            host_endpoint: None,
            ..Default::default()
        };

        let result = flatten_node_info(&ni);
        let Err(err) = result else {
            panic!("expected session_token credentials to be rejected");
        };

        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("BMC endpoint credentials"));
        assert!(err.message.contains("session_token"));
        assert!(err.message.contains("user_pass"));
    }

    #[test]
    fn flat_endpoint_debug_redacts_passwords() {
        let flat = FlatEndpoint {
            bmc: Some(FlatEndpointConfig {
                username: "bmc_user".into(),
                password: "bmc-secret".into(),
                ..Default::default()
            }),
            host: Some(FlatEndpointConfig {
                username: "host_user".into(),
                password: "host-secret".into(),
                ..Default::default()
            }),
        };

        let debug = format!("{flat:?}");

        assert!(debug.contains("bmc_user"));
        assert!(debug.contains("host_user"));
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("bmc-secret"));
        assert!(!debug.contains("host-secret"));
    }

    #[test]
    fn creds_for_node_type_picks_host_for_switch_bmc_otherwise() {
        let flat = FlatEndpoint {
            bmc: Some(FlatEndpointConfig {
                username: "bmc_user".into(),
                password: "bmc_pass".into(),
                ..Default::default()
            }),
            host: Some(FlatEndpointConfig {
                username: "host_user".into(),
                password: "host_pass".into(),
                ..Default::default()
            }),
        };
        assert_eq!(
            flat.creds_for_node_type(DomainNodeType::SwitchGb200Nvidia),
            Some(("host_user", "host_pass"))
        );
        assert_eq!(
            flat.creds_for_node_type(DomainNodeType::ComputeGb200Nvidia),
            Some(("bmc_user", "bmc_pass"))
        );
        assert_eq!(
            flat.creds_for_node_type(DomainNodeType::PowershelfGb200Liteon),
            Some(("bmc_user", "bmc_pass"))
        );
    }

    #[test]
    fn resolve_bmc_port_defaults_to_443_when_unset() -> RmsResult<()> {
        let flat = FlatEndpoint {
            bmc: Some(FlatEndpointConfig {
                port: 0,
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(flat.resolve_bmc_port()?, 443);

        Ok(())
    }

    #[test]
    fn resolve_bmc_port_accepts_valid_range() -> RmsResult<()> {
        let flat = FlatEndpoint {
            bmc: Some(FlatEndpointConfig {
                port: 443,
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(flat.resolve_bmc_port()?, 443);

        let flat = FlatEndpoint {
            bmc: Some(FlatEndpointConfig {
                port: 65535,
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(flat.resolve_bmc_port()?, 65535);

        let flat = FlatEndpoint {
            bmc: Some(FlatEndpointConfig {
                port: 1,
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(flat.resolve_bmc_port()?, 1);

        Ok(())
    }

    #[test]
    fn resolve_port_for_node_type_uses_host_port_for_switch() -> RmsResult<()> {
        let flat = FlatEndpoint {
            bmc: Some(FlatEndpointConfig {
                port: 443,
                ..Default::default()
            }),
            host: Some(FlatEndpointConfig {
                port: 8443,
                ..Default::default()
            }),
        };

        assert_eq!(
            flat.resolve_port_for_node_type(DomainNodeType::SwitchGb200Nvidia)?,
            8443
        );
        assert_eq!(
            flat.resolve_port_for_node_type(DomainNodeType::ComputeGb200Nvidia)?,
            443
        );

        Ok(())
    }

    #[test]
    fn resolve_port_for_node_type_rejects_missing_host_endpoint_for_switch() -> RmsResult<()> {
        let flat = FlatEndpoint {
            bmc: Some(FlatEndpointConfig {
                port: 9443,
                ..Default::default()
            }),
            ..Default::default()
        };

        let err = flat
            .resolve_port_for_node_type(DomainNodeType::SwitchGb200Nvidia)
            .expect_err("expected missing host endpoint error");

        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert_eq!(err.message, "missing host endpoint for switch");
        assert_eq!(
            flat.resolve_port_for_node_type(DomainNodeType::ComputeGb200Nvidia)?,
            9443
        );

        Ok(())
    }

    #[test]
    fn resolve_port_for_node_type_defaults_explicit_host_endpoint_to_443() -> RmsResult<()> {
        let flat = FlatEndpoint {
            bmc: Some(FlatEndpointConfig {
                port: 9443,
                ..Default::default()
            }),
            host: Some(FlatEndpointConfig {
                port: 0,
                ..Default::default()
            }),
        };

        assert_eq!(
            flat.resolve_port_for_node_type(DomainNodeType::SwitchGb200Nvidia)?,
            443
        );

        Ok(())
    }

    #[test]
    fn tls_override_for_switch_prefers_explicit_host_endpoint() {
        let flat = FlatEndpoint {
            bmc: Some(FlatEndpointConfig {
                dangerously_accept_invalid_certs: true,
                ..Default::default()
            }),
            host: Some(FlatEndpointConfig {
                dangerously_accept_invalid_certs: false,
                ..Default::default()
            }),
        };

        assert!(
            !flat.dangerously_accept_invalid_certs_for_node_type(DomainNodeType::SwitchGb200Nvidia)
        );
        assert!(
            flat.dangerously_accept_invalid_certs_for_node_type(DomainNodeType::ComputeGb200Nvidia)
        );
    }

    #[test]
    fn tls_override_for_switch_does_not_fallback_to_bmc() {
        let flat = FlatEndpoint {
            bmc: Some(FlatEndpointConfig {
                dangerously_accept_invalid_certs: true,
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(
            !flat.dangerously_accept_invalid_certs_for_node_type(DomainNodeType::SwitchGb200Nvidia)
        );
        assert!(
            flat.dangerously_accept_invalid_certs_for_node_type(DomainNodeType::ComputeGb200Nvidia)
        );
    }

    #[test]
    fn optional_bmc_endpoint_accepts_missing_mac_address() {
        let flat = FlatEndpoint {
            bmc: Some(FlatEndpointConfig {
                ip_address: "10.0.0.1".into(),
                port: 443,
                ..Default::default()
            }),
            ..Default::default()
        };

        let result = flat.optional_bmc_endpoint();
        let Ok(Some(endpoint)) = result else {
            panic!("expected optional BMC endpoint without MAC to be accepted");
        };

        assert_eq!(endpoint.endpoint.ip_address, "10.0.0.1");
        assert_eq!(endpoint.endpoint.mac_address, "");
    }

    #[test]
    fn optional_bmc_endpoint_rejects_missing_ip_address() {
        let flat = FlatEndpoint {
            bmc: Some(FlatEndpointConfig {
                mac_address: "aa:bb:cc:dd:ee:ff".into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let err = flat
            .optional_bmc_endpoint()
            .expect_err("expected missing BMC endpoint ip_address error");

        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("BMC endpoint ip_address"));
    }

    #[test]
    fn required_bmc_endpoint_rejects_missing_mac_address() {
        let flat = FlatEndpoint {
            bmc: Some(FlatEndpointConfig {
                ip_address: "10.0.0.1".into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let err = flat
            .bmc_endpoint()
            .expect_err("expected required BMC endpoint to require a MAC address");

        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("BMC endpoint mac_address"));
    }

    #[test]
    fn optional_host_endpoint_accepts_missing_mac_address() {
        let flat = FlatEndpoint {
            host: Some(FlatEndpointConfig {
                ip_address: "10.0.1.1".into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let result = flat.optional_host_endpoint();
        let Ok(Some(endpoint)) = result else {
            panic!("expected host endpoint without MAC to be accepted");
        };

        assert_eq!(endpoint.endpoint.ip_address, "10.0.1.1");
        assert_eq!(endpoint.endpoint.mac_address, "");
    }

    #[test]
    fn optional_host_endpoint_rejects_missing_ip_address() {
        let flat = FlatEndpoint {
            host: Some(FlatEndpointConfig {
                mac_address: "aa:bb:cc:00:01:11".into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let result = flat.optional_host_endpoint();
        let Err(err) = result else {
            panic!("expected missing host endpoint ip_address error");
        };

        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("host endpoint ip_address"));
    }

    #[test]
    fn resolve_bmc_port_rejects_out_of_range() {
        let flat = FlatEndpoint {
            bmc: Some(FlatEndpointConfig {
                port: 65536,
                ..Default::default()
            }),
            ..Default::default()
        };
        let err = flat
            .resolve_bmc_port()
            .expect_err("expected out-of-range error");

        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert!(err.message.contains("65536"), "unexpected message: {err}");
        assert!(err.message.contains("1-65535"), "unexpected message: {err}");

        let flat = FlatEndpoint {
            bmc: Some(FlatEndpointConfig {
                port: u32::MAX,
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(flat.resolve_bmc_port().is_err());
    }

    #[test]
    fn resolve_bmc_port_rejects_missing_bmc_endpoint() {
        let flat = FlatEndpoint::default();

        let err = flat
            .resolve_bmc_port()
            .expect_err("expected missing BMC endpoint error");

        assert_eq!(err.code, ErrorCode::InvalidArgument);
        assert_eq!(err.message, "missing BMC endpoint");
    }

    #[test]
    fn resolve_management_port_maps_switch_ssh_port_to_default_nvue_port() -> RmsResult<()> {
        let flat = FlatEndpoint {
            host: Some(FlatEndpointConfig {
                ip_address: "10.0.1.1".into(),
                port: 22,
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            flat.resolve_management_port_for_node_type(DomainNodeType::SwitchGb200Nvidia)?,
            443
        );

        let endpoint = flat.switch_host_management_endpoint()?;

        assert_eq!(endpoint.endpoint.port, 443);
        assert!(!endpoint.dangerously_accept_invalid_certs);

        Ok(())
    }

    #[test]
    fn switch_host_name_falls_back_to_ip_when_host_name_missing() -> RmsResult<()> {
        let flat = FlatEndpoint {
            host: Some(FlatEndpointConfig {
                ip_address: "10.0.1.1".into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(flat.switch_host_name()?, "10.0.1.1");

        let flat = FlatEndpoint {
            host: Some(FlatEndpointConfig {
                ip_address: "10.0.1.1".into(),
                host_name: Some("switch.example.com".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(flat.switch_host_name()?, "switch.example.com");

        Ok(())
    }

    #[test]
    fn resolve_management_port_preserves_non_switch_ports() -> RmsResult<()> {
        let flat = FlatEndpoint {
            bmc: Some(FlatEndpointConfig {
                port: 22,
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            flat.resolve_management_port_for_node_type(DomainNodeType::ComputeGb200Nvidia)?,
            22
        );
        assert_eq!(
            flat.resolve_management_port_for_node_type(DomainNodeType::PowershelfGb200Liteon)?,
            22
        );

        Ok(())
    }

    #[test]
    fn resolve_management_port_preserves_custom_switch_api_port() -> RmsResult<()> {
        let flat = FlatEndpoint {
            host: Some(FlatEndpointConfig {
                port: 8443,
                ..Default::default()
            }),
            ..Default::default()
        };

        assert_eq!(
            flat.resolve_management_port_for_node_type(DomainNodeType::SwitchGb200Nvidia)?,
            8443
        );

        Ok(())
    }

    #[test]
    fn flatten_host_endpoint_only_switch_has_no_bmc_creds() -> RmsResult<()> {
        let ni = pb::NodeInfo {
            node_id: "sw-01".into(),
            rack_id: "r".into(),
            r#type: Some(pb::NodeType::SwitchGb200Nvidia as i32),
            bmc_endpoint: None,
            host_endpoint: Some(pb::Endpoint {
                interface: Some(pb::NetworkInterface {
                    ip_address: "10.0.1.1".into(),
                    mac_address: "aa:bb:cc:00:01:11".into(),
                    host_name: None,
                }),
                port: 22,
                credentials: Some(user_pass_creds("host_user", "host_pass")),
            }),
            ..Default::default()
        };

        let flat = flatten_node_info(&ni)?;

        assert!(flat.bmc.is_none());
        assert!(flat.host.is_some());
        assert_eq!(
            flat.host.as_ref().map(|e| e.username.as_str()),
            Some("host_user")
        );
        assert_eq!(
            flat.host.as_ref().map(|e| e.password.expose_secret()),
            Some("host_pass")
        );
        assert_eq!(flat.host.as_ref().map(|e| e.port), Some(22));
        assert_eq!(
            flat.resolve_port_for_node_type(DomainNodeType::SwitchGb200Nvidia)?,
            22
        );
        assert_eq!(
            flat.creds_for_node_type(DomainNodeType::SwitchGb200Nvidia),
            Some(("host_user", "host_pass"))
        );

        Ok(())
    }

    #[test]
    fn power_op_conversions() {
        let cases = [
            (pb::PowerOperation::On, PowerOp::On),
            (pb::PowerOperation::Off, PowerOp::Off),
            (pb::PowerOperation::Reset, PowerOp::PowerCycle),
            (pb::PowerOperation::ForceOn, PowerOp::ForceOn),
            (pb::PowerOperation::ForceOff, PowerOp::ForceOff),
            (
                pb::PowerOperation::GracefulShutdown,
                PowerOp::GracefulShutdown,
            ),
            (
                pb::PowerOperation::GracefulRestart,
                PowerOp::GracefulRestart,
            ),
            (pb::PowerOperation::ForceRestart, PowerOp::ForceRestart),
        ];

        for (proto, domain) in cases {
            assert_eq!(PowerOp::try_from(proto), Ok(domain));
        }

        assert_eq!(
            PowerOp::try_from(pb::PowerOperation::Unspecified),
            Err(PowerOperationConversionError::Unspecified)
        );
    }

    #[test]
    fn rack_power_op_conversions() {
        let cases = [
            (pb::RackPowerOperation::On, PowerOp::On),
            (pb::RackPowerOperation::Off, PowerOp::Off),
            (pb::RackPowerOperation::Cycle, PowerOp::PowerCycle),
        ];

        for (proto, domain) in cases {
            assert_eq!(PowerOp::try_from(proto), Ok(domain));
        }

        assert_eq!(
            PowerOp::try_from(pb::RackPowerOperation::Unspecified),
            Err(PowerOperationConversionError::Unspecified)
        );
    }

    #[test]
    fn power_state_strings() {
        assert_eq!(power_state_to_string(PowerState::On), "ON");
        assert_eq!(power_state_to_string(PowerState::Off), "OFF");
        assert_eq!(power_state_to_string(PowerState::Unknown), "UNKNOWN");
    }

    #[test]
    fn node_type_conversions() {
        assert_eq!(
            proto_node_type_to_string(pb::NodeType::ComputeGb200Nvidia as i32),
            Some("compute_gb200_nvidia")
        );
        assert_eq!(
            proto_node_type_to_string(pb::NodeType::ComputeGb300Nvidia as i32),
            Some("compute_gb300_nvidia")
        );
        assert_eq!(
            proto_node_type_to_string(pb::NodeType::SwitchGb200Nvidia as i32),
            Some("switch_gb200_nvidia")
        );
        assert_eq!(
            proto_node_type_to_string(pb::NodeType::SwitchGb300Nvidia as i32),
            Some("switch_gb300_nvidia")
        );
        assert_eq!(
            proto_node_type_to_string(pb::NodeType::ComputeGb300Lenovo as i32),
            Some("compute_gb300_lenovo")
        );
        assert_eq!(
            proto_node_type_to_string(pb::NodeType::ComputeVrnvl72Nvidia as i32),
            Some("compute_vrnvl72_nvidia")
        );
        assert_eq!(
            proto_node_type_to_string(pb::NodeType::PowershelfGb200Liteon as i32),
            Some("powershelf_gb200_liteon")
        );
        assert_eq!(
            proto_node_type_to_string(pb::NodeType::PowershelfGb200Delta as i32),
            Some("powershelf_gb200_delta")
        );
        assert_eq!(
            proto_node_type_to_string(pb::NodeType::PowershelfGb300Liteon as i32),
            Some("powershelf_gb300_liteon")
        );
        assert_eq!(
            proto_node_type_to_string(pb::NodeType::PowershelfGb300Delta as i32),
            Some("powershelf_gb300_delta")
        );
        assert_eq!(
            proto_node_type_to_string(pb::NodeType::Unspecified as i32),
            None
        );
        assert_eq!(
            proto_node_type_to_domain(pb::NodeType::ComputeGb300Nvidia as i32),
            Some(DomainNodeType::ComputeGb300Nvidia)
        );
        assert_eq!(
            proto_node_type_to_domain(pb::NodeType::SwitchGb200Nvidia as i32),
            Some(DomainNodeType::SwitchGb200Nvidia)
        );
        assert_eq!(
            proto_node_type_to_domain(pb::NodeType::SwitchGb300Nvidia as i32),
            Some(DomainNodeType::SwitchGb300Nvidia)
        );
        assert_eq!(
            proto_node_type_to_domain(pb::NodeType::ComputeGb300Lenovo as i32),
            Some(DomainNodeType::ComputeGb300Lenovo)
        );
        assert_eq!(
            proto_node_type_to_domain(pb::NodeType::ComputeVrnvl72Nvidia as i32),
            Some(DomainNodeType::ComputeVrnvl72Nvidia)
        );
        assert_eq!(
            proto_node_type_to_domain(pb::NodeType::SwitchVrnvl72Nvidia as i32),
            Some(DomainNodeType::SwitchVrnvl72Nvidia)
        );
        assert_eq!(
            proto_node_type_to_string(pb::NodeType::SwitchVrnvl72Nvidia as i32),
            Some("switch_vrnvl72_nvidia")
        );

        assert_eq!(
            domain_node_type_to_proto(DomainNodeType::ComputeGb300Nvidia),
            pb::NodeType::ComputeGb300Nvidia
        );
        assert_eq!(
            domain_node_type_to_proto(DomainNodeType::ComputeGb200Wiwynn),
            pb::NodeType::Unspecified
        );
        assert_eq!(
            domain_node_type_to_proto(DomainNodeType::SwitchGb300Nvidia),
            pb::NodeType::SwitchGb300Nvidia
        );
        assert_eq!(
            domain_node_type_to_proto(DomainNodeType::PowershelfGb300Liteon),
            pb::NodeType::PowershelfGb300Liteon
        );
        assert_eq!(
            domain_node_type_to_proto(DomainNodeType::ComputeVrnvl72Nvidia),
            pb::NodeType::ComputeVrnvl72Nvidia
        );
        assert_eq!(
            domain_node_type_to_proto(DomainNodeType::SwitchVrnvl72Nvidia),
            pb::NodeType::SwitchVrnvl72Nvidia
        );
    }
}
