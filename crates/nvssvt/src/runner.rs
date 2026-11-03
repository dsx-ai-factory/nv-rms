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

//! Launches NVSSVT scans through the launcher API and tracks them to
//! completion. Process management and result-artifact parsing belong to
//! NVSSVT's own `nvssvt_server.py`, which owns the subprocess and the log
//! directory; this crate only speaks HTTP to it.

use serde_json::Value;

use crate::client::{LaunchRequestBody, NvssvtHttpClient, RunStatusBody, redacted};
use crate::config::NvssvtConfig;
use crate::error::NvssvtError;

/// Fields needed to launch an NVSSVT run, mirroring the subset of
/// `nvssvt_server.py`'s `LaunchRequest` that RMS drives (itself a mapping of
/// NVSSVT CLI flags, see `src/nvssvt_cli_help.py` in the NVSSVT repo).
/// Everything not listed here falls back to NVSSVT's own CLI defaults.
#[derive(Clone, Default)]
pub struct StartScanFields {
    pub bmc_ip: Option<String>,
    pub bmc_user: Option<String>,
    pub bmc_pwd: Option<String>,
    pub host_ip: Option<String>,
    pub host_user: Option<String>,
    pub host_pwd: Option<String>,
    /// NVSSVT qualification level(s) to run.
    pub quallevel: Vec<String>,
    /// NVSSVT's `--config-override`, passed through untouched.
    pub config_override: Option<String>,
}

impl std::fmt::Debug for StartScanFields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Destructured without `..` so a new field cannot be added without
        // deciding here whether it is secret.
        let Self {
            bmc_ip,
            bmc_user,
            bmc_pwd,
            host_ip,
            host_user,
            host_pwd,
            quallevel,
            config_override,
        } = self;
        f.debug_struct("StartScanFields")
            .field("bmc_ip", bmc_ip)
            .field("bmc_user", bmc_user)
            .field("bmc_pwd", &redacted(bmc_pwd))
            .field("host_ip", host_ip)
            .field("host_user", host_user)
            .field("host_pwd", &redacted(host_pwd))
            .field("quallevel", quallevel)
            .field("config_override", config_override)
            .finish()
    }
}

impl From<StartScanFields> for LaunchRequestBody {
    fn from(fields: StartScanFields) -> Self {
        Self {
            run_id: None,
            bmc_ip: fields.bmc_ip,
            bmc_user: fields.bmc_user,
            bmc_pwd: fields.bmc_pwd,
            host_ip: fields.host_ip,
            host_user: fields.host_user,
            host_pwd: fields.host_pwd,
            quallevel: fields.quallevel,
            config_override: fields.config_override,
        }
    }
}

/// A scan accepted by the launcher. `pid` and `log_dir` describe the NVSSVT
/// process and its artifacts *on the launcher's side*, so they are reported
/// verbatim rather than interpreted -- `log_dir` is a path in the NVSSVT
/// container's filesystem, not one this process can open.
#[derive(Debug, Clone)]
pub struct LaunchOutcome {
    pub run_id: String,
    pub pid: i64,
    pub log_dir: String,
    /// RFC 3339 timestamp as produced by the launcher.
    pub started_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    Starting,
    Running,
    Completed,
    Failed,
    /// A state string this client does not recognize. Treated as
    /// not-yet-terminal by callers, so a launcher that grows a new
    /// intermediate state keeps polling rather than failing the scan.
    Unknown,
}

impl RunState {
    fn from_api(state: &str) -> Self {
        match state {
            "starting" => Self::Starting,
            "running" => Self::Running,
            "completed" => Self::Completed,
            "failed" => Self::Failed,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScanStatus {
    pub run_id: String,
    pub pid: i64,
    pub state: RunState,
    pub return_code: Option<i32>,
    /// RFC 3339 timestamp as produced by the launcher.
    pub started_at: String,
    /// NVSSVT's log directory, as a path on the launcher's filesystem.
    pub log_dir: String,
    pub overall_progress: Option<f64>,
    pub duts: Value,
    pub summary: Option<Value>,
    pub error: Option<String>,
}

impl From<RunStatusBody> for ScanStatus {
    fn from(body: RunStatusBody) -> Self {
        Self {
            run_id: body.run_id,
            pid: body.pid,
            state: RunState::from_api(&body.state),
            return_code: body.return_code,
            started_at: body.started_at,
            log_dir: body.log_dir,
            overall_progress: body.overall_progress,
            duts: body.duts,
            summary: body.summary,
            error: body.error,
        }
    }
}

/// Launches NVSSVT scans through the launcher API and tracks them to
/// completion.
pub struct NvssvtRunner {
    client: NvssvtHttpClient,
}

impl NvssvtRunner {
    pub fn new(config: NvssvtConfig) -> Result<Self, NvssvtError> {
        Ok(Self {
            client: NvssvtHttpClient::new(&config)?,
        })
    }

    /// Base URL this runner talks to, for logging.
    pub fn base_url(&self) -> &str {
        self.client.base_url()
    }

    /// Asks the launcher to start an NVSSVT run for the given DUT/config
    /// attributes.
    ///
    /// `run_id` names the run: pass the caller's own identifier for the scan
    /// (RMS passes its job id) so tracking needs no id mapping on either
    /// side. The launcher generates an id when this is `None`, and rejects an
    /// id already in use with [`NvssvtError::RunAlreadyExists`]. Either way
    /// the id actually used comes back on [`LaunchOutcome::run_id`].
    pub async fn launch(
        &self,
        run_id: Option<&str>,
        fields: StartScanFields,
    ) -> Result<LaunchOutcome, NvssvtError> {
        let body = LaunchRequestBody {
            run_id: run_id.map(str::to_string),
            ..fields.into()
        };
        let body = self.client.launch_run(&body).await?;
        Ok(LaunchOutcome {
            run_id: body.run_id,
            pid: body.pid,
            log_dir: body.log_dir,
            started_at: body.started_at,
        })
    }

    /// Tracks progress of a previously launched NVSSVT run and returns
    /// results once available.
    pub async fn status(&self, run_id: &str) -> Result<ScanStatus, NvssvtError> {
        Ok(self.client.get_run_status(run_id).await?.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields_with_passwords() -> StartScanFields {
        StartScanFields {
            bmc_ip: Some("10.0.0.5".to_owned()),
            bmc_user: Some("admin".to_owned()),
            bmc_pwd: Some("bmc-secret".to_owned()),
            host_ip: Some("10.0.1.5".to_owned()),
            host_user: Some("root".to_owned()),
            host_pwd: Some("host-secret".to_owned()),
            ..Default::default()
        }
    }

    #[test]
    fn start_scan_fields_debug_redacts_passwords() {
        let debug = format!("{:?}", fields_with_passwords());

        assert!(!debug.contains("bmc-secret"), "{debug}");
        assert!(!debug.contains("host-secret"), "{debug}");
        assert!(debug.contains("[REDACTED]"), "{debug}");
        assert!(debug.contains("admin") && debug.contains("root"), "{debug}");
    }

    #[test]
    fn launch_request_body_debug_redacts_passwords() {
        let body = LaunchRequestBody::from(fields_with_passwords());
        let debug = format!("{body:?}");

        assert!(!debug.contains("bmc-secret"), "{debug}");
        assert!(!debug.contains("host-secret"), "{debug}");
        assert!(debug.contains("[REDACTED]"), "{debug}");
        // Redaction is Debug-only: the launch request must still carry them.
        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(json["bmc_pwd"], "bmc-secret");
        assert_eq!(json["host_pwd"], "host-secret");
    }

    #[test]
    fn unrecognized_states_are_not_terminal() {
        assert_eq!(RunState::from_api("running"), RunState::Running);
        assert_eq!(RunState::from_api("queued"), RunState::Unknown);
        assert_eq!(RunState::from_api(""), RunState::Unknown);
    }

    #[test]
    fn launch_fields_map_onto_the_launcher_json_keys() {
        let body: LaunchRequestBody = StartScanFields {
            bmc_ip: Some("10.0.0.5".to_string()),
            quallevel: vec!["NICo".to_string()],
            ..Default::default()
        }
        .into();
        let json = serde_json::to_value(&body).expect("serialize");

        assert_eq!(json["bmc_ip"], "10.0.0.5");
        assert_eq!(json["quallevel"][0], "NICo");
        // An unset run id is omitted, leaving the launcher to generate one;
        // likewise an unset config override, leaving NVSSVT's own default.
        assert!(json.get("run_id").is_none());
        assert!(json.get("config_override").is_none());
        // Unset optionals are omitted so the launcher applies its own CLI
        // defaults rather than receiving explicit nulls.
        assert!(json.get("bmc_user").is_none());
    }
}
