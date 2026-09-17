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

//! Verification workflows run after a switch configuration job reports
//! success, confirming the change actually converged before the caller's
//! job is marked complete.
//!
//! SDN fabric state, following the VR NVL72 bring-up guide's state-report
//! procedure. Triggered over SSH: the captured NVUE OpenAPI contract
//! (Cumulus Linux 5.12) has no `sdn`, `app-state`, or report-generation
//! route, and this repository's own NVOS-extension NVUE endpoints
//! (`/cluster/apps/{app}`, `/sdn/factory-default`) do not cover it either,
//! so there is no REST equivalent to fall back to.

use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};

use super::SwitchGb200Nvidia;
// Reuses the firmware-filename charset (alphanumeric/-/_/.) rather than a
// dedicated validator: it matches the documented state-report filename
// format and tolerates a future extension without a second validator to
// keep in sync.
use super::validation::is_valid_firmware_filename as is_valid_report_filename;
use crate::transport::ssh_client::SshClient;
use crate::utilities::error::{Result, RmsError};

use nvue_client::cluster::ClusterNodeServerAddresses;

use serde::Deserialize;

const GENERATE_REPORT_COMMAND: &str =
    "nv action generate sdn state apps nmx-controller type nv-bridge-state-report";

const REPORT_DIR: &str = "/host/cluster_infra/app_state/nmx-controller/nv-bridge-state-report";

const CONNECTION_STATE_CONNECTED: &str = "NMX_NVB_CONNECTION_STATE_CONNECTED";
const KA_STATE_OK: &str = "NMX_NVB_KA_STATE_OK";
const ACTIVE_STATE_ACTIVE: &str = "NMX_NVB_ACTIVE_STATE_ACTIVE";

#[derive(Debug, Deserialize)]
struct SdnStateReport {
    #[serde(rename = "statusUpdates")]
    status_updates: Vec<SdnConnectionGroup>,
}

#[derive(Debug, Deserialize)]
struct SdnConnectionGroup {
    #[serde(rename = "nvbdUuid")]
    group_id: String,

    #[serde(rename = "connectionStatuses")]
    connection_statuses: Vec<SdnConnectionStatus>,
}

#[derive(Debug, Deserialize)]
struct SdnConnectionStatus {
    #[serde(rename = "nvbsUuid")]
    peer_id: String,

    #[serde(rename = "nvbsAddress")]
    peer_address: String,

    // The three state fields and chassis_sn_mismatch are proto3 enums/bool
    // in the wire schema (nmx-m-nmx-c.proto), whose JSON encoding omits a
    // field entirely when it holds its zero/default value. `default` lets a
    // genuinely UNKNOWN-state or unset-mismatch connection deserialize (as
    // "" or false) and fail the health check below on its own merits,
    // instead of a missing key failing the whole report's parse.
    #[serde(rename = "connectionState", default)]
    connection_state: String,

    #[serde(rename = "kaState", default)]
    ka_state: String,

    #[serde(rename = "activeState", default)]
    active_state: String,

    #[serde(rename = "chassisSnMismatch", default)]
    chassis_sn_mismatch: bool,
}

impl SwitchGb200Nvidia {
    /// Verifies every SDN fabric connection reported by the bring-up guide's
    /// state-report procedure is live and reaches each configured
    /// primary-server peer, per its post-NMX-C-config check.
    ///
    /// Only needs to run on one switch: the report covers every NVLink
    /// switch's fabric connection state cluster-wide. Peer addresses are
    /// compared as a subset check rather than an exact count, since one
    /// remote chassis can report multiple tray addresses for a single
    /// configured peer IP; the safety property that matters is that every
    /// configured peer has at least one live connection, not an exact
    /// address-count match.
    pub(crate) async fn verify_sdn_fabric_state(
        &self,
        expected_peers: &ClusterNodeServerAddresses,
    ) -> Result<()> {
        // The directory may not exist yet if this switch has never generated
        // a report before. Testing for it in the shell keeps that case a
        // successful empty listing, which leaves every error below worth
        // propagating. Tolerating an error class here instead cannot express
        // the distinction: ErrorCode::Internal covers a command killed by a
        // signal, a missing exit status, and a failed exec request as well
        // as `ls` reporting no such file, so accepting it would clear
        // previous_filename on a broken channel and let the freshness check
        // below accept a stale report as new.
        let previous_filename = self
            .ssh_exec(&format!(
                "if [ -d {REPORT_DIR} ]; then ls -t {REPORT_DIR}; fi"
            ))
            .await?
            .lines()
            .next()
            .unwrap_or("")
            .trim()
            .to_owned();

        self.ssh_exec(GENERATE_REPORT_COMMAND).await?;

        let listing = self.ssh_exec(&format!("ls -t {REPORT_DIR}")).await?;
        let filename = listing.lines().next().unwrap_or("").trim();

        if !is_valid_report_filename(filename) {
            return Err(RmsError::internal(format!(
                "no SDN state report found in {REPORT_DIR}"
            )));
        }

        // A generate command can exit successfully without nmx-controller
        // actually writing a new report (for example, while its service is
        // degraded). Require the newest file to have changed so a stale
        // report from an earlier run cannot pass this check silently.
        if filename == previous_filename {
            return Err(RmsError::internal(format!(
                "SDN state report in {REPORT_DIR} did not refresh after report generation"
            )));
        }

        let contents = self
            .ssh_exec(&format!("cat {REPORT_DIR}/{filename}"))
            .await?;

        let report: SdnStateReport = serde_json::from_str(&contents).map_err(|error| {
            RmsError::internal(format!("failed to parse SDN state report: {error}"))
        })?;

        for group in &report.status_updates {
            for status in &group.connection_statuses {
                if status.connection_state != CONNECTION_STATE_CONNECTED
                    || status.ka_state != KA_STATE_OK
                    || status.active_state != ACTIVE_STATE_ACTIVE
                {
                    return Err(RmsError::internal(format!(
                        "SDN connection to '{}' ({}) in group '{}' is unhealthy: \
                         connectionState={}, kaState={}, activeState={}",
                        status.peer_id,
                        status.peer_address,
                        group.group_id,
                        status.connection_state,
                        status.ka_state,
                        status.active_state
                    )));
                }

                if status.chassis_sn_mismatch {
                    return Err(RmsError::internal(format!(
                        "SDN connection to '{}' ({}) in group '{}' is live but reports a \
                         chassis serial number mismatch",
                        status.peer_id, status.peer_address, group.group_id
                    )));
                }
            }
        }

        let observed_peers: HashSet<IpAddr> = report
            .status_updates
            .iter()
            .flat_map(|group| &group.connection_statuses)
            .filter_map(|status| match status.peer_address.parse::<SocketAddr>() {
                Ok(address) => Some(address.ip()),
                Err(error) => {
                    // A healthy connection with an address we can't parse
                    // would otherwise surface only as "peer missing," which
                    // hides that the real cause is a parse failure, not an
                    // absent connection.
                    tracing::warn!(
                        peer_id = %status.peer_id,
                        peer_address = %status.peer_address,
                        %error,
                        "SDN state report peer address did not parse as ip:port"
                    );

                    None
                }
            })
            .collect();

        let missing: Vec<&IpAddr> = expected_peers.difference(&observed_peers).collect();

        if !missing.is_empty() {
            return Err(RmsError::internal(format!(
                "SDN state report has no live connection to configured primary \
                 server(s): {missing:?}"
            )));
        }

        Ok(())
    }

    /// Runs one SSH command against this switch, routing through the test
    /// hook when set.
    async fn ssh_exec(&self, command: &str) -> Result<String> {
        #[cfg(test)]
        if let Some(exec) = &self.ssh_exec_for_test {
            return exec(command);
        }

        let ssh = SshClient::connect(self.ssh_endpoint()?, SshClient::DEFAULT_TIMEOUT).await?;

        ssh.exec(command, SshClient::DEFAULT_TIMEOUT).await
    }
}
