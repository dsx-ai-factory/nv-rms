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

pub use ::redfish_client::{NvidiaMnnvlinkTopology, RedfishClient, ResetType};

use crate::domain::node::PowerState;
use crate::domain::rack::EndpointConfig;
use crate::utilities::error::{ErrorCode, Result, RmsError};

use ::redfish_client::{BmcCredentials, RedfishError, RedfishPowerState};
use secrecy::ExposeSecret;

/// Build a shared Redfish client from RMS endpoint config.
pub fn redfish_client_from_endpoint_config(config: &EndpointConfig) -> Result<RedfishClient> {
    RedfishClient::new(
        &config.endpoint.ip_address,
        config.endpoint.port,
        credentials_from_endpoint_config(config)?,
        config.dangerously_accept_invalid_certs,
        true,
    )
    .map_err(RmsError::from)
}

fn credentials_from_endpoint_config(config: &EndpointConfig) -> Result<BmcCredentials> {
    let Some(credentials) = config.credentials.as_ref() else {
        return Err(RmsError::invalid_argument(
            "BMC credentials are required for Redfish operations",
        ));
    };

    if credentials.username.is_empty() || credentials.password.expose_secret().is_empty() {
        return Err(RmsError::invalid_argument(
            "BMC username and password must not be empty for Redfish operations",
        ));
    }

    Ok(BmcCredentials::username_password(
        credentials.username.clone(),
        Some(credentials.password.expose_secret().to_owned()),
    ))
}

impl From<Option<RedfishPowerState>> for PowerState {
    fn from(power_state: Option<RedfishPowerState>) -> Self {
        match power_state {
            Some(RedfishPowerState::On) => Self::On,
            Some(RedfishPowerState::Off) => Self::Off,
            Some(
                RedfishPowerState::PoweringOn
                | RedfishPowerState::PoweringOff
                | RedfishPowerState::Paused
                | RedfishPowerState::UnsupportedValue,
            )
            | None => Self::Unknown,
        }
    }
}

impl From<RedfishError> for RmsError {
    fn from(error: RedfishError) -> Self {
        let code = match &error {
            RedfishError::AlreadyExists(_) => ErrorCode::AlreadyExists,
            RedfishError::ConnectionRefused(_) => ErrorCode::ConnectionRefused,
            RedfishError::DnsResolutionFailed(_) => ErrorCode::DnsResolutionFailed,
            RedfishError::FailedPrecondition(_) => ErrorCode::FailedPrecondition,
            RedfishError::Forbidden(_) => ErrorCode::FailedPrecondition,
            RedfishError::Internal(_) => ErrorCode::Internal,
            RedfishError::InvalidEndpoint { .. } => ErrorCode::InvalidArgument,
            RedfishError::InvalidArgument(_) => ErrorCode::InvalidArgument,
            RedfishError::NotFound(_) => ErrorCode::NotFound,
            RedfishError::Timeout(_) => ErrorCode::Timeout,
            RedfishError::Unauthenticated(_) => ErrorCode::Unauthenticated,
            RedfishError::Unavailable(_) => ErrorCode::Unavailable,
            RedfishError::CreateHttpClient { .. } => ErrorCode::Internal,
            RedfishError::OpenFirmwareFile { source, .. }
            | RedfishError::ReadFirmwareFileMetadata { source, .. } => match source.kind() {
                std::io::ErrorKind::NotFound => ErrorCode::NotFound,
                std::io::ErrorKind::PermissionDenied => ErrorCode::FailedPrecondition,
                std::io::ErrorKind::TimedOut => ErrorCode::Timeout,
                _ => ErrorCode::Internal,
            },
            _ => ErrorCode::Internal,
        };

        Self::new(code, error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::domain::rack::{Endpoint, EndpointCredentials};

    fn endpoint_config() -> EndpointConfig {
        EndpointConfig::with_credentials(
            Endpoint {
                ip_address: "fd00::1".to_owned(),
                mac_address: "aa:bb:cc:dd:ee:ff".to_owned(),
                port: 8443,
                host_name: None,
            },
            Some(EndpointCredentials::new("admin", "secret")),
            true,
        )
    }

    #[test]
    fn credentials_from_config_preserve_endpoint_credentials() {
        let credentials = credentials_from_endpoint_config(&endpoint_config()).unwrap();

        assert_eq!(
            credentials,
            BmcCredentials::username_password("admin".to_owned(), Some("secret".to_owned()))
        );

        assert!(!format!("{credentials:?}").contains("secret"));
    }

    #[test]
    fn endpoint_config_without_credentials_is_rejected() {
        let mut config = endpoint_config();
        config.credentials = None;

        let Err(error) = redfish_client_from_endpoint_config(&config) else {
            panic!("missing credentials should be rejected");
        };

        assert_eq!(error.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn endpoint_config_with_empty_credentials_is_rejected() {
        let mut config = endpoint_config();
        config.credentials = Some(EndpointCredentials::new("", ""));

        let Err(error) = redfish_client_from_endpoint_config(&config) else {
            panic!("empty credentials should be rejected");
        };

        assert_eq!(error.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn shared_error_maps_to_rms_error() {
        let result = RedfishClient::new(
            "\n",
            443,
            BmcCredentials::username_password("admin".to_owned(), Some("secret".to_owned())),
            true,
            true,
        )
        .map_err(RmsError::from)
        .map(|_| ());

        let error = result.unwrap_err();

        assert_eq!(error.code, ErrorCode::InvalidArgument);
    }
}
