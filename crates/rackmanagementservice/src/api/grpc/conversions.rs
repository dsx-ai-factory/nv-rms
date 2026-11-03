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

use crate::api::grpc::bmc_tls::apply_bmc_redfish_tls_policy;
use crate::domain::node::{NodeType as DomainNodeType, PowerOp, PowerState};
use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials};
use crate::orchestrator::job_lifecycle::{JobError, JobState};
use crate::orchestrator::job_tracker::{JobInfo, RejectedBatchTarget};
use crate::utilities::error::{Result as RmsResult, RmsError};
use chrono::{DateTime, Utc};
use librms::protos::rack_manager as pb;
use thiserror::Error;

const SUPPLEMENTAL_HOST_SSH_PORT: u32 = 22;

// Batch node results
/// Keys each device of a batch request by its `(rack_id, node_id)` for
/// [`JobTracker::create_batch_jobs`](crate::orchestrator::job_tracker::JobTracker::create_batch_jobs),
/// which hands the device back with the job it reserves.
pub(crate) fn batch_targets(devices: Vec<pb::NodeInfo>) -> Vec<(String, String, pb::NodeInfo)> {
    devices
        .into_iter()
        .map(|device| (device.rack_id.clone(), device.node_id.clone(), device))
        .collect()
}

/// Turns the rejections of
/// [`JobTracker::create_batch_jobs`](crate::orchestrator::job_tracker::JobTracker::create_batch_jobs)
/// into the failure entries of a `NodeBatchResponse`.
///
/// `NodeOperationResult` keys results by node alone, so the target's `rack_id`
/// is dropped here; it stays on [`RejectedBatchTarget`] for callers that need to
/// correlate a refusal back to the rack it came from.
pub(crate) fn failed_node_results(
    rejected: Vec<RejectedBatchTarget>,
) -> impl Iterator<Item = pb::NodeOperationResult> {
    rejected.into_iter().map(|target| pb::NodeOperationResult {
        node_id: target.node_id,
        status: pb::ReturnCode::Failure.into(),
        error_message: target.error_message,
    })
}

// Power conversions
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

pub fn power_state_to_string(state: PowerState) -> &'static str {
    match state {
        PowerState::On => "ON",
        PowerState::Off => "OFF",
        PowerState::Unknown => "UNKNOWN",
    }
}

// Node type conversions
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
        // Wiwynn and Dell GB200 are descriptor-only on the wire. Returning Unspecified
        // keeps the descriptor authoritative without extending the protobuf enum.
        DomainNodeType::ComputeGb200Wiwynn | DomainNodeType::ComputeGb200Dell => {
            pb::NodeType::Unspecified
        }
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
        // The VR LiteOn power shelf is descriptor-only on the wire. Returning
        // Unspecified keeps the descriptor authoritative without adding a
        // protobuf enum value.
        DomainNodeType::PowershelfVrnvl72Liteon => pb::NodeType::Unspecified,
        // The VR Delta power shelf is descriptor-only on the wire for the same
        // reason: the descriptor stays authoritative without a protobuf enum
        // value.
        DomainNodeType::PowershelfVrnvl72Delta => pb::NodeType::Unspecified,
    }
}

pub fn proto_node_type_to_string(nt: i32) -> Option<&'static str> {
    match pb::NodeType::try_from(nt).ok()? {
        pb::NodeType::Unspecified => None,
        supported => proto_node_type_to_domain(supported as i32).map(DomainNodeType::as_str),
    }
}

// NodeInfo / NodeUpdateInfo flattening
/// Flattened endpoint values used by legacy `NodeConfig` construction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FlatEndpointConfig {
    pub ip_address: String,
    pub mac_address: String,
    pub host_name: Option<String>,
    pub port: u32,
    pub credentials: Option<EndpointCredentials>,
}

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
    /// BMC Redfish operations do not use it. TLS verification is the RMS
    /// `[bmc] insecure_bmc` policy, not a request field.
    pub fn optional_bmc_endpoint(&self, insecure_bmc: bool) -> RmsResult<Option<EndpointConfig>> {
        let Some(endpoint) = self.bmc.as_ref() else {
            return Ok(None);
        };

        Ok(Some(apply_bmc_redfish_tls_policy(
            Self::endpoint_from_config(EndpointKind::Bmc, endpoint, false)?,
            insecure_bmc,
        )))
    }

    /// Return the required BMC endpoint with RMS BMC TLS policy applied.
    pub fn bmc_endpoint(&self, insecure_bmc: bool) -> RmsResult<EndpointConfig> {
        let Some(endpoint) = self.bmc.as_ref() else {
            return Err(RmsError::invalid_argument("missing BMC endpoint"));
        };

        Ok(apply_bmc_redfish_tls_policy(
            Self::endpoint_from_config(EndpointKind::Bmc, endpoint, true)?,
            insecure_bmc,
        ))
    }

    /// Return the required host endpoint.
    ///
    /// Host MAC is optional for direct switch requests that only need host IP,
    /// port, and credentials.
    pub fn host_endpoint(&self) -> RmsResult<EndpointConfig> {
        let Some(endpoint) = self.host.as_ref() else {
            return Err(RmsError::invalid_argument("missing host endpoint"));
        };

        Self::endpoint_from_config(EndpointKind::Host, endpoint, false)
    }

    /// Return the optional host endpoint when the request provided one.
    ///
    /// Host MAC is optional for direct switch requests that only need host IP,
    /// port, and credentials.
    pub fn optional_host_endpoint(&self) -> RmsResult<Option<EndpointConfig>> {
        let Some(endpoint) = self.host.as_ref() else {
            return Ok(None);
        };

        Self::endpoint_from_config(EndpointKind::Host, endpoint, false).map(Some)
    }

    /// Return the optional compute host endpoint used by SSH-based workflows.
    /// An omitted port means the standard SSH port instead of the management
    /// API default used by the existing generic endpoint conversion.
    pub fn optional_compute_host_ssh_endpoint(&self) -> RmsResult<Option<EndpointConfig>> {
        let Some(endpoint) = self.host.as_ref() else {
            return Ok(None);
        };

        let mut endpoint = endpoint.clone();
        if endpoint.port == 0 {
            endpoint.port = 22;
        }

        Self::endpoint_from_config(EndpointKind::Host, &endpoint, false).map(Some)
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
    /// is absent, has no credentials, or uses session-token authentication.
    pub fn creds_for_node_type(&self, node_type: DomainNodeType) -> Option<(&str, &str)> {
        let endpoint = if node_type.uses_host_management_endpoint() {
            self.host.as_ref()
        } else {
            self.bmc.as_ref()
        }?;

        endpoint.credentials.as_ref()?.user_pass()
    }

    /// Return whether the node's management endpoint has complete credentials.
    ///
    /// Switch management requires host username/password credentials. BMC
    /// management also accepts a non-empty Redfish session token.
    pub fn has_credentials_for_node_type(&self, node_type: DomainNodeType) -> bool {
        if node_type.uses_host_management_endpoint() {
            return self.creds_for_node_type(node_type).is_some();
        }

        self.bmc
            .as_ref()
            .and_then(|endpoint| endpoint.credentials.as_ref())
            .is_some_and(EndpointCredentials::has_auth_material)
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

    /// Switch inventories may specify SSH port 22 in the shared host endpoint.
    /// NVUE uses HTTPS, so its connection and reservation use port 443 instead.
    pub(crate) fn switch_management_port(port: u16) -> u16 {
        if port == 22 { 443 } else { port }
    }

    fn endpoint_from_config(
        endpoint_kind: EndpointKind,
        endpoint: &FlatEndpointConfig,
        require_mac_address: bool,
    ) -> RmsResult<EndpointConfig> {
        // Name the specific missing field so a per-node failure is actionable
        // rather than an opaque "missing fields". Requiredness is
        // context-dependent (see `flat_endpoint_config`): ip is always
        // required here, mac only when `require_mac_address` is set.
        let name = endpoint_kind.label();

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

        Ok(EndpointConfig::with_credentials(
            Endpoint {
                ip_address: endpoint.ip_address.clone(),
                mac_address: endpoint.mac_address.clone(),
                port: Self::resolve_port(endpoint.port)?,
                host_name: endpoint.host_name.clone(),
            },
            endpoint.credentials.clone(),
            // Host/NVUE always verifies. BMC skip-verify is applied in
            // `bmc_endpoint` / `optional_bmc_endpoint` from `[bmc]
            // insecure_bmc`; request fields cannot change that policy.
            false,
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

        if node_type.uses_host_management_endpoint() {
            Ok(Self::switch_management_port(port))
        } else {
            Ok(port)
        }
    }
}

/// Endpoint role, used to decide which auth modes are valid -- not just to
/// label error messages. Keeping this as a type (rather than a `&str`
/// compared against a literal) means renaming a display label can never
/// silently change which endpoints accept session-token auth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndpointKind {
    Bmc,
    Host,

    /// Request-scoped host whose purpose is defined by the consuming RPC.
    SupplementalHost,
}

impl EndpointKind {
    fn label(self) -> &'static str {
        match self {
            Self::Bmc => "BMC",
            Self::Host => "host",
            Self::SupplementalHost => "additional host",
        }
    }

    /// Only BMC endpoints support Redfish session-token auth; NVUE/SSH host
    /// endpoints require username/password.
    fn supports_session_token(self) -> bool {
        matches!(self, Self::Bmc)
    }
}

fn extract_endpoint_credentials(
    endpoint_kind: EndpointKind,
    credentials: Option<&pb::Credentials>,
) -> RmsResult<Option<EndpointCredentials>> {
    let Some(auth) = credentials.and_then(|c| c.auth.as_ref()) else {
        return Ok(None);
    };

    match auth {
        // Reject only the asymmetric case (exactly one field set): a fully empty
        // pair is "no credentials provided" and is returned as None below so it
        // never reaches the transport layer as a (bogus) empty Basic auth header.
        // A half-empty pair is never valid Basic auth.
        pb::credentials::Auth::UserPass(up) if up.username.is_empty() != up.password.is_empty() => {
            Err(RmsError::invalid_argument(
                "user_pass credentials must include both a non-empty username and password",
            ))
        }
        pb::credentials::Auth::UserPass(up) if up.username.is_empty() && up.password.is_empty() => {
            Ok(None)
        }
        pb::credentials::Auth::UserPass(up) => Ok(Some(EndpointCredentials::new(
            up.username.clone(),
            up.password.clone(),
        ))),
        pb::credentials::Auth::SessionToken(token)
            if endpoint_kind.supports_session_token() && !token.is_empty() =>
        {
            Ok(Some(EndpointCredentials::from_session_token(token.clone())))
        }
        pb::credentials::Auth::SessionToken(_) if endpoint_kind.supports_session_token() => {
            Err(RmsError::invalid_argument(format!(
                "{} endpoint session_token must not be empty",
                endpoint_kind.label()
            )))
        }
        pb::credentials::Auth::SessionToken(_) => Err(RmsError::invalid_argument(format!(
            "{} endpoint does not support session token auth; use basic auth (user/pass credentials) instead",
            endpoint_kind.label()
        ))),
    }
}

fn flat_endpoint_config(
    endpoint_kind: EndpointKind,
    endpoint: &pb::Endpoint,
) -> RmsResult<FlatEndpointConfig> {
    let iface = endpoint.interface.as_ref();
    let credentials = extract_endpoint_credentials(endpoint_kind, endpoint.credentials.as_ref())?;

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
        credentials,
    })
}

/// Convert one supplemental host endpoint into request-scoped SSH settings.
///
/// Supplemental host endpoints use port 22 when the protobuf port is omitted
/// and reject other ports because the current SSH transport connects only to
/// port 22. They require username/password credentials because the switch image
/// recovery skeleton does not support Redfish session tokens or unauthenticated
/// access.
pub(crate) fn additional_host_ssh_endpoint(endpoint: &pb::Endpoint) -> RmsResult<EndpointConfig> {
    if endpoint.port != 0 && endpoint.port != SUPPLEMENTAL_HOST_SSH_PORT {
        return Err(RmsError::invalid_argument(format!(
            "additional host SSH endpoint port must be 22 when specified, got {}",
            endpoint.port
        )));
    }

    let mut flattened = flat_endpoint_config(EndpointKind::SupplementalHost, endpoint)?;

    if flattened.port == 0 {
        flattened.port = SUPPLEMENTAL_HOST_SSH_PORT;
    }

    let endpoint =
        FlatEndpoint::endpoint_from_config(EndpointKind::SupplementalHost, &flattened, false)?;

    if endpoint
        .credentials
        .as_ref()
        .and_then(EndpointCredentials::user_pass_secret)
        .is_none()
    {
        return Err(RmsError::invalid_argument(
            "additional host endpoint requires non-empty username/password credentials",
        ));
    }

    Ok(endpoint)
}

/// Flatten endpoint fields from a protobuf `NodeInfo`.
///
/// # Errors
///
/// Returns an `InvalidArgument` error when a host endpoint uses token auth or
/// when a BMC session token is empty.
pub fn flatten_node_info(ni: &pb::NodeInfo) -> RmsResult<FlatEndpoint> {
    Ok(FlatEndpoint {
        bmc: ni
            .bmc_endpoint
            .as_ref()
            .map(|endpoint| flat_endpoint_config(EndpointKind::Bmc, endpoint))
            .transpose()?,
        host: ni
            .host_endpoint
            .as_ref()
            .map(|endpoint| flat_endpoint_config(EndpointKind::Host, endpoint))
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

    // flatten_node_info credential source
    fn user_pass_creds(u: &str, p: &str) -> pb::Credentials {
        pb::Credentials {
            auth: Some(pb::credentials::Auth::UserPass(pb::UsernamePassword {
                username: u.into(),
                password: p.into(),
            })),
        }
    }

    #[test]
    fn additional_host_ssh_endpoint_uses_ssh_default_and_preserves_credentials() -> RmsResult<()> {
        let endpoint = pb::Endpoint {
            interface: Some(pb::NetworkInterface {
                ip_address: "192.0.2.50".to_owned(),
                mac_address: "aa:bb:cc:00:00:50".to_owned(),
                host_name: Some("onie-switch.example.com".to_owned()),
            }),
            port: 0,
            credentials: Some(user_pass_creds("onie", "recovery-password")),
        };

        let endpoint = additional_host_ssh_endpoint(&endpoint)?;

        assert_eq!(endpoint.endpoint.ip_address, "192.0.2.50");
        assert_eq!(endpoint.endpoint.port, 22);

        assert_eq!(
            endpoint
                .credentials
                .as_ref()
                .and_then(EndpointCredentials::user_pass),
            Some(("onie", "recovery-password"))
        );

        Ok(())
    }

    #[test]
    fn additional_host_ssh_endpoint_requires_username_password_credentials() {
        let endpoint = pb::Endpoint {
            interface: Some(pb::NetworkInterface {
                ip_address: "192.0.2.50".to_owned(),
                ..Default::default()
            }),
            port: 22,
            credentials: None,
        };

        let result = additional_host_ssh_endpoint(&endpoint);

        let Err(error) = result else {
            panic!("missing ONIE credentials should be rejected");
        };

        assert_eq!(error.code, ErrorCode::InvalidArgument);
        assert!(error.message.contains("additional host endpoint"));
        assert!(error.message.contains("username/password"));
    }

    #[test]
    fn additional_host_ssh_endpoint_rejects_session_token_credentials() {
        let endpoint = pb::Endpoint {
            interface: Some(pb::NetworkInterface {
                ip_address: "192.0.2.50".to_owned(),
                ..Default::default()
            }),
            port: 0,
            credentials: Some(pb::Credentials {
                auth: Some(pb::credentials::Auth::SessionToken("token".to_owned())),
            }),
        };

        let result = additional_host_ssh_endpoint(&endpoint);

        let Err(error) = result else {
            panic!("supplemental SSH endpoint must reject session-token credentials");
        };

        assert_eq!(error.code, ErrorCode::InvalidArgument);

        assert!(
            error
                .message
                .contains("additional host endpoint does not support session token auth")
        );
    }

    #[test]
    fn additional_host_ssh_endpoint_rejects_unsupported_port() {
        let endpoint = pb::Endpoint {
            interface: Some(pb::NetworkInterface {
                ip_address: "192.0.2.50".to_owned(),
                ..Default::default()
            }),
            port: 2222,
            credentials: Some(user_pass_creds("onie", "recovery-password")),
        };

        let result = additional_host_ssh_endpoint(&endpoint);

        let Err(error) = result else {
            panic!("unsupported ONIE SSH port should be rejected");
        };

        assert_eq!(error.code, ErrorCode::InvalidArgument);
        assert!(error.message.contains("port must be 22"));
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
            flat.bmc
                .as_ref()
                .and_then(|endpoint| endpoint.credentials.as_ref())
                .and_then(EndpointCredentials::user_pass),
            Some(("bmc_user", "bmc_pass"))
        );

        assert_eq!(
            flat.host
                .as_ref()
                .and_then(|endpoint| endpoint.credentials.as_ref())
                .and_then(EndpointCredentials::user_pass),
            Some(("host_user", "host_pass"))
        );

        Ok(())
    }

    #[test]
    fn flatten_from_proto_bmc_skips_verify_and_switch_host_does_not() -> RmsResult<()> {
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

        assert!(flat.bmc_endpoint(true)?.dangerously_accept_invalid_certs);
        assert!(!flat.bmc_endpoint(false)?.dangerously_accept_invalid_certs);
        assert!(
            !flat
                .switch_host_management_endpoint()?
                .dangerously_accept_invalid_certs
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
        assert!(err.message.contains("host endpoint"));
        assert!(err.message.contains("session token"));
        assert!(err.message.contains("basic auth"));
    }

    #[test]
    fn flatten_accepts_bmc_session_token_credentials() -> RmsResult<()> {
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

        let flat = flatten_node_info(&ni)?;

        assert!(flat.has_credentials_for_node_type(DomainNodeType::ComputeGb200Nvidia));

        let endpoint = flat.bmc_endpoint(true)?;

        assert!(endpoint.dangerously_accept_invalid_certs);

        let Some(credentials) = endpoint.credentials.as_ref() else {
            panic!("BMC credentials should be preserved");
        };

        let EndpointCredentials::BmcSessionToken(token) = credentials else {
            panic!("BMC session token should be preserved");
        };

        assert_eq!(secrecy::ExposeSecret::expose_secret(token), "token");

        Ok(())
    }

    #[test]
    fn flatten_rejects_empty_bmc_session_token_credentials() {
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
                    auth: Some(pb::credentials::Auth::SessionToken(String::new())),
                }),
            }),
            host_endpoint: None,
            ..Default::default()
        };

        let Err(error) = flatten_node_info(&ni) else {
            panic!("empty BMC session token should be rejected");
        };

        assert_eq!(error.code, ErrorCode::InvalidArgument);
        assert!(error.message.contains("must not be empty"));
    }

    #[test]
    fn flatten_rejects_user_pass_credentials_with_empty_username() {
        // An empty-username UserPass credential must be rejected outright, not
        // silently accepted and later reinterpreted as session-token auth by a
        // downstream consumer.
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
                credentials: Some(user_pass_creds("", "secret")),
            }),
            host_endpoint: None,
            ..Default::default()
        };

        let Err(error) = flatten_node_info(&ni) else {
            panic!("empty-username user_pass credentials should be rejected");
        };

        assert_eq!(error.code, ErrorCode::InvalidArgument);
        assert!(error.message.contains("non-empty username"));
    }

    #[test]
    fn flatten_treats_fully_empty_user_pass_credentials_as_absent() {
        // A fully empty username/password pair means "no credentials provided"
        // and must resolve to None, not a UserPass credential that would later
        // send an unauthenticated (empty) Basic auth header over the wire.
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
                credentials: Some(user_pass_creds("", "")),
            }),
            host_endpoint: None,
            ..Default::default()
        };

        let flat = flatten_node_info(&ni).expect("fully empty user_pass should not be rejected");
        assert!(flat.bmc.expect("bmc endpoint").credentials.is_none());
    }

    #[test]
    fn flat_endpoint_debug_redacts_passwords() {
        let flat = FlatEndpoint {
            bmc: Some(FlatEndpointConfig {
                credentials: Some(EndpointCredentials::new("bmc_user", "bmc-secret")),
                ..Default::default()
            }),
            host: Some(FlatEndpointConfig {
                credentials: Some(EndpointCredentials::new("host_user", "host-secret")),
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
                credentials: Some(EndpointCredentials::new("bmc_user", "bmc_pass")),
                ..Default::default()
            }),
            host: Some(FlatEndpointConfig {
                credentials: Some(EndpointCredentials::new("host_user", "host_pass")),
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
    fn optional_bmc_endpoint_accepts_missing_mac_address() {
        let flat = FlatEndpoint {
            bmc: Some(FlatEndpointConfig {
                ip_address: "10.0.0.1".into(),
                port: 443,
                ..Default::default()
            }),
            ..Default::default()
        };

        let result = flat.optional_bmc_endpoint(true);
        let Ok(Some(endpoint)) = result else {
            panic!("expected optional BMC endpoint without MAC to be accepted");
        };

        assert_eq!(endpoint.endpoint.ip_address, "10.0.0.1");
        assert_eq!(endpoint.endpoint.mac_address, "");
        assert!(endpoint.dangerously_accept_invalid_certs);
        assert!(
            !flat
                .optional_bmc_endpoint(false)
                .expect("verify-mode optional BMC")
                .expect("present")
                .dangerously_accept_invalid_certs
        );
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
            .optional_bmc_endpoint(true)
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
            .bmc_endpoint(true)
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
    fn optional_compute_host_ssh_endpoint_defaults_to_port_22() {
        let flat = FlatEndpoint {
            host: Some(FlatEndpointConfig {
                ip_address: "10.0.1.1".into(),
                ..Default::default()
            }),
            ..Default::default()
        };

        let endpoint = flat
            .optional_compute_host_ssh_endpoint()
            .expect("compute host endpoint should be valid")
            .expect("compute host endpoint should exist");

        assert_eq!(endpoint.endpoint.port, 22);
    }

    #[test]
    fn optional_compute_host_ssh_endpoint_preserves_explicit_port() {
        let flat = FlatEndpoint {
            host: Some(FlatEndpointConfig {
                ip_address: "10.0.1.1".into(),
                port: 2222,
                ..Default::default()
            }),
            ..Default::default()
        };

        let endpoint = flat
            .optional_compute_host_ssh_endpoint()
            .expect("compute host endpoint should be valid")
            .expect("compute host endpoint should exist");

        assert_eq!(endpoint.endpoint.port, 2222);
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
            flat.host
                .as_ref()
                .and_then(|endpoint| endpoint.credentials.as_ref())
                .and_then(EndpointCredentials::user_pass),
            Some(("host_user", "host_pass"))
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

    #[test]
    fn vr_liteon_power_shelf_has_no_proto_enum_value() {
        assert_eq!(
            domain_node_type_to_proto(DomainNodeType::PowershelfVrnvl72Liteon),
            pb::NodeType::Unspecified
        );
        assert_eq!(
            proto_node_type_to_domain(pb::NodeType::Unspecified as i32),
            None
        );
        assert!(
            pb::NodeType::from_str_name("POWERSHELF_VRNVL72_LITEON").is_none(),
            "adding a proto enum value requires wiring proto_node_type_to_domain"
        );
    }

    #[test]
    fn vr_delta_power_shelf_has_no_proto_enum_value() {
        assert_eq!(
            domain_node_type_to_proto(DomainNodeType::PowershelfVrnvl72Delta),
            pb::NodeType::Unspecified
        );
        assert!(
            pb::NodeType::from_str_name("POWERSHELF_VRNVL72_DELTA").is_none(),
            "adding a proto enum value requires wiring proto_node_type_to_domain"
        );
        assert_eq!(
            DomainNodeType::PowershelfVrnvl72Delta.as_str(),
            "powershelf_vrnvl72_delta"
        );
    }
}
