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

//! HTTP client for NVSSVT's launcher API (`nvssvt_server.py`). Field names in
//! [`LaunchRequestBody`] intentionally match the FastAPI `LaunchRequest`
//! model's JSON keys.
//!
//! The body carries only the parameters RMS actually sets -- DUT credentials,
//! the qualification level, and an optional config override. Every other NVSSVT knob is omitted, so NVSSVT
//! applies its own CLI defaults; add a field here (and to
//! [`crate::runner::StartScanFields`]) when RMS needs to drive one.

use std::time::Duration;

use reqwest::{Client, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::config::NvssvtConfig;
use crate::error::NvssvtError;

/// How long an idle connection may be kept for reuse. Comfortably under
/// uvicorn's 5s keep-alive default, so this side always closes first.
const IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone, Default, Serialize)]
pub(crate) struct LaunchRequestBody {
    /// Identifier to track the run under. The launcher generates one when
    /// this is unset, and rejects an id already in use with `409`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bmc_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bmc_user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bmc_pwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_pwd: Option<String>,
    pub quallevel: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_override: Option<String>,
}

/// What `Debug` prints in place of a password.
pub(crate) fn redacted(secret: &Option<String>) -> Option<&'static str> {
    secret.as_ref().map(|_| "[REDACTED]")
}

impl std::fmt::Debug for LaunchRequestBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Destructured without `..` so a new field cannot be added without
        // deciding here whether it is secret.
        let Self {
            run_id,
            bmc_ip,
            bmc_user,
            bmc_pwd,
            host_ip,
            host_user,
            host_pwd,
            quallevel,
            config_override,
        } = self;
        f.debug_struct("LaunchRequestBody")
            .field("run_id", run_id)
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

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct LaunchResponseBody {
    pub run_id: String,
    pub pid: i64,
    pub log_dir: String,
    pub started_at: String,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct RunStatusBody {
    pub run_id: String,
    pub pid: i64,
    pub state: String,
    pub return_code: Option<i32>,
    pub started_at: String,
    pub log_dir: String,
    pub overall_progress: Option<f64>,
    #[serde(default)]
    pub duts: serde_json::Value,
    pub summary: Option<serde_json::Value>,
    pub error: Option<String>,
}

/// Thin async client over `nvssvt_server.py`'s two endpoints: `POST /runs`
/// to launch a scan and `GET /runs/{run_id}` to poll it.
pub(crate) struct NvssvtHttpClient {
    http: Client,
    base_url: String,
}

impl NvssvtHttpClient {
    pub(crate) fn new(config: &NvssvtConfig) -> Result<Self, NvssvtError> {
        let http = Client::builder()
            .timeout(config.request_timeout)
            // The launcher is uvicorn, whose keep-alive timeout defaults to
            // 5s -- the same order as the caller's poll interval. Reusing a
            // pooled connection the server is closing at that moment races,
            // and surfaces as a send failure on a perfectly healthy
            // launcher, so idle connections are dropped well before uvicorn
            // touches them. One request every few seconds does not need a
            // pool anyway.
            .pool_idle_timeout(IDLE_CONNECTION_TIMEOUT)
            .build()
            .map_err(NvssvtError::BuildClient)?;
        Ok(Self {
            http,
            base_url: config.base_url.trim_end_matches('/').to_string(),
        })
    }

    pub(crate) fn base_url(&self) -> &str {
        &self.base_url
    }

    pub(crate) async fn launch_run(
        &self,
        req: &LaunchRequestBody,
    ) -> Result<LaunchResponseBody, NvssvtError> {
        let url = format!("{}/runs", self.base_url);
        let resp = self
            .http
            .post(url)
            .json(req)
            .send()
            .await
            .map_err(NvssvtError::Transport)?;
        Self::parse(resp).await
    }

    pub(crate) async fn get_run_status(&self, run_id: &str) -> Result<RunStatusBody, NvssvtError> {
        let url = format!("{}/runs/{}", self.base_url, run_id);
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(NvssvtError::Transport)?;
        Self::parse(resp).await
    }

    async fn parse<T: DeserializeOwned>(resp: reqwest::Response) -> Result<T, NvssvtError> {
        let status = resp.status();
        if status == StatusCode::NOT_FOUND {
            let body = resp.text().await.unwrap_or_default();
            return Err(NvssvtError::RunNotFound(body));
        }
        if status == StatusCode::CONFLICT {
            let body = resp.text().await.unwrap_or_default();
            return Err(NvssvtError::RunAlreadyExists(body));
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(NvssvtError::Upstream { status, body });
        }
        resp.json::<T>().await.map_err(NvssvtError::Decode)
    }
}
