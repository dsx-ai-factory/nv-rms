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

//! Handlers for ConfigureSwitchCertificate and GetConfigureSwitchCertificateJobStatus.

use std::sync::Arc;
use std::time::Duration;

use super::server::RackManagerServiceImpl;
use crate::api::grpc::conversions::{
    flatten_node_info, proto_node_type_to_domain, proto_node_type_to_string,
    timestamp_from_datetime,
};
use crate::domain::node::{NodeKind, NodeType as DomainNodeType};
use crate::domain::rack::NodeConfig;
use crate::libgnmi::{
    Endpoint as GnmiEndpoint, Gnmi, GnmiClientPool, GnmiCredentials, GnmiTlsConfig,
    capabilities_connect_error_message, capabilities_rpc_error_message, capabilities_target_uri,
    is_capabilities_connect_error,
};
use crate::libnmxc::{Endpoint, NmxcClientPool, NmxcTlsConfig, gateway_id_from_env};
use crate::nodes::switch_gb200_nvidia::{SwitchGb200Nvidia, grpc_port_for_app};
use crate::nodes::switch_gb300_nvidia::SwitchGb300Nvidia;
use crate::nodes::switch_mtls::{
    StableNmxcTlsConfig, SwitchMtlsMaterialPaths, SwitchMtlsService, mtls_remote_file_specs,
    sftp_copy_stage_name,
};
use crate::orchestrator::job_tracker::{JobError, JobTracker, JobType};
use crate::orchestrator::stage_timeline::StageTimeline;
use crate::utilities::error::{Result, RmsError};
use crate::utilities::url::grpc_target_uri;
use librms::protos::rack_manager as rm;

use serde_json::{Value, json};

fn parse_switch_services(services: &[i32]) -> std::result::Result<Vec<SwitchMtlsService>, String> {
    if services.is_empty() {
        return Err("services must contain at least one SwitchService".into());
    }

    let mut parsed = Vec::with_capacity(services.len());
    for &raw in services {
        let proto = rm::SwitchService::try_from(raw)
            .map_err(|_| format!("invalid SwitchService value: {raw}"))?;
        let Some(service) = SwitchMtlsService::from_proto(proto) else {
            return Err("services must not include SWITCH_SERVICE_UNSPECIFIED".into());
        };
        if !parsed.contains(&service) {
            parsed.push(service);
        }
    }
    Ok(parsed)
}

fn validate_test_hello_services(
    test_hello: bool,
    services: &[SwitchMtlsService],
) -> std::result::Result<(), String> {
    if !test_hello {
        return Ok(());
    }
    let has_nvue = services.contains(&SwitchMtlsService::NvueApi);
    let has_nmx = services.contains(&SwitchMtlsService::ScaleUpFabricManager);
    let has_gnmi = services.contains(&SwitchMtlsService::ScaleUpFabricTelemetryInterface);
    if !has_nvue && !has_nmx && !has_gnmi {
        return Err("test_hello requires SWITCH_SERVICE_NVUE_API, \
             SWITCH_SERVICE_SCALE_UP_FABRIC_MANAGER, and/or \
             SWITCH_SERVICE_SCALE_UP_FABRIC_TELEMETRY_INTERFACE in services"
            .into());
    }
    Ok(())
}

fn use_registered_nvue_client(credentials_match: bool, binds_nvue_mtls: bool) -> Result<bool> {
    if credentials_match {
        return Ok(true);
    }

    if binds_nvue_mtls {
        return Err(RmsError::failed_precondition(
            "NVUE mTLS installation credentials must match the registered switch",
        ));
    }

    Ok(false)
}

const NVUE_HELLO_ENDPOINT: &str = "/nvue_v1/system";
const NVUE_HELLO_CONNECT_ATTEMPTS: u32 = 5;
const NVUE_HELLO_CONNECT_RETRY_WAIT: Duration = Duration::from_secs(6);

const NMX_CONTROLLER_APP: &str = "nmx-controller";
const NMX_HELLO_CONNECT_ATTEMPTS: u32 = 5;
const NMX_HELLO_CONNECT_RETRY_WAIT: Duration = Duration::from_secs(6);

const GNMI_CAPABILITIES_CONNECT_ATTEMPTS: u32 = 5;
const GNMI_CAPABILITIES_CONNECT_RETRY_WAIT: Duration = Duration::from_secs(6);
const STAGE_UNSET_MTLS_MODE: &str = "unset_mtls_mode";

#[derive(Clone)]
struct NvueTlsTransition {
    tls: nvue_client::ClientTls,
    server_name: String,
    fingerprint: [u8; 32],
}

struct SwitchCertificateInstallMode {
    domain: String,
    material: SwitchMtlsMaterialPaths,
    client_tls: Option<StableNmxcTlsConfig>,
    nvue_client_tls: Option<nvue_client::ClientTls>,
}

enum SwitchCertificateMode {
    // Secure mode installs switch-side certificates and configures selected
    // services to require mTLS.
    Install(Box<SwitchCertificateInstallMode>),

    // Insecure switch mode is an opt-out from certificate management. The same
    // RPC creates unset jobs so switches are moved back to non-mTLS service mode.
    UnsetMtls,
}

impl SwitchCertificateMode {
    fn domain_for_log(&self) -> &str {
        match self {
            Self::Install(install) => &install.domain,
            Self::UnsetMtls => "(insecure-switch)",
        }
    }

    fn is_unset_mtls(&self) -> bool {
        matches!(self, Self::UnsetMtls)
    }

    fn job_label(&self) -> &'static str {
        match self {
            Self::Install(_) => "switch certificate",
            Self::UnsetMtls => "switch mTLS unset",
        }
    }
}

fn nmx_hello_target_uri(switch_host: &str, use_tls: bool) -> String {
    grpc_target_uri(switch_host, grpc_port_for_app(NMX_CONTROLLER_APP), use_tls)
}

fn nmx_hello_connect_error(err: &RmsError) -> bool {
    err.message.contains("NMX Hello connect failed")
}

fn nvue_hello_connect_error(err: &RmsError) -> bool {
    SwitchGb200Nvidia::is_nvue_retryable_error(err)
}

async fn test_nvue_hello_with_retries(switch: &SwitchGb200Nvidia) -> Result<()> {
    let mut last_err = None;
    for attempt in 0..NVUE_HELLO_CONNECT_ATTEMPTS {
        match switch.verify_nvue_api_hello().await {
            Ok(()) => {
                tracing::info!(
                    node = %switch.id(),
                    endpoint = NVUE_HELLO_ENDPOINT,
                    "NVUE API connectivity test succeeded"
                );
                return Ok(());
            }
            Err(e) if nvue_hello_connect_error(&e) && attempt + 1 < NVUE_HELLO_CONNECT_ATTEMPTS => {
                tracing::info!(
                    node = %switch.id(),
                    attempt = attempt + 1,
                    max_attempts = NVUE_HELLO_CONNECT_ATTEMPTS,
                    retry_in_secs = NVUE_HELLO_CONNECT_RETRY_WAIT.as_secs(),
                    error = %e.message,
                    "NVUE API connectivity test failed; retrying after mTLS bind"
                );
                last_err = Some(e);
                tokio::time::sleep(NVUE_HELLO_CONNECT_RETRY_WAIT).await;
            }
            Err(e) => return Err(e),
        }
    }

    Err(last_err
        .unwrap_or_else(|| RmsError::internal("NVUE API connectivity test failed after retries")))
}

async fn activate_nvue_client_tls_with_retries(
    client: &nvue_client::Client,
    mut prepared: nvue_client::PreparedClientTls,
    transition: &NvueTlsTransition,
) -> Result<()> {
    let mut last_error = None;

    for attempt in 0..NVUE_HELLO_CONNECT_ATTEMPTS {
        match client
            .activate_prepared_client_tls(
                &prepared,
                NVUE_HELLO_ENDPOINT,
                nvue_client::DEFAULT_TIMEOUT,
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(nvue_client::ClientError::StalePreparedTransport)
                if attempt + 1 < NVUE_HELLO_CONNECT_ATTEMPTS =>
            {
                prepared = client
                    .prepare_client_tls(transition.tls.clone(), Some(&transition.server_name))
                    .await?;

                if prepared.material_fingerprint() != transition.fingerprint {
                    return Err(RmsError::failed_precondition(
                        "RMS NVUE client TLS material changed during certificate installation",
                    ));
                }
            }
            Err(error) => {
                let error = RmsError::from(error);

                if !nvue_hello_connect_error(&error) || attempt + 1 == NVUE_HELLO_CONNECT_ATTEMPTS {
                    return Err(error);
                }

                tracing::info!(
                    attempt = attempt + 1,
                    max_attempts = NVUE_HELLO_CONNECT_ATTEMPTS,
                    retry_in_secs = NVUE_HELLO_CONNECT_RETRY_WAIT.as_secs(),
                    error = %error.message,
                    "prepared NVUE client TLS is not ready; retrying"
                );

                last_error = Some(error);
                tokio::time::sleep(NVUE_HELLO_CONNECT_RETRY_WAIT).await;
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        RmsError::internal("prepared NVUE client TLS activation failed after retries")
    }))
}

async fn prepare_nvue_tls_transition(
    switch: &SwitchGb200Nvidia,
    tls: Option<&nvue_client::ClientTls>,
    server_name: String,
) -> Result<Option<NvueTlsTransition>> {
    let Some(tls) = tls else {
        return Ok(None);
    };

    let prepared = switch
        .nvue_client()?
        .prepare_client_tls(tls.clone(), Some(&server_name))
        .await?;

    Ok(Some(NvueTlsTransition {
        tls: tls.clone(),
        server_name,
        fingerprint: prepared.material_fingerprint(),
    }))
}

async fn bind_nvue_mtls_with_client_transition(
    switch: &SwitchGb200Nvidia,
    material: &SwitchMtlsMaterialPaths,
    transition: &NvueTlsTransition,
) -> Result<()> {
    if switch.mtls_skip_nvue_for_test() {
        return Ok(());
    }

    let client = switch.nvue_client()?;
    let prepared = client
        .prepare_client_tls(transition.tls.clone(), Some(&transition.server_name))
        .await?;

    if prepared.material_fingerprint() != transition.fingerprint {
        return Err(RmsError::failed_precondition(
            "RMS NVUE client TLS material changed during certificate installation",
        ));
    }

    let revision_id = switch.stage_nvue_mtls_binding(material).await?;
    let apply = switch.nvue_start_config_revision(&revision_id).await;
    let may_have_applied = match &apply {
        Ok(()) => true,
        Err(error) => SwitchGb200Nvidia::is_nvue_retryable_error(error),
    };

    if !may_have_applied {
        return apply;
    }

    let activation = activate_nvue_client_tls_with_retries(client, prepared, transition).await;

    match (apply, activation) {
        (_, Ok(())) => {
            switch.nvue_finish_config_revision(&revision_id).await?;
            Ok(())
        }
        (Ok(()), Err(error)) => Err(error),
        (Err(apply_error), Err(activation_error)) => Err(RmsError::internal(format!(
            "NVUE mTLS apply may have started but the prepared client failed: {}; apply error: {}",
            activation_error.message, apply_error.message
        ))),
    }
}

/// Ensures the cluster is up and nmx-controller gRPC is exposed before Hello.
async fn prepare_nmx_controller_for_hello(
    switch: &SwitchGb200Nvidia,
    switch_host: &str,
) -> Result<()> {
    switch.set_cluster_state(true).await.map_err(|e| {
        RmsError::internal(format!(
            "NMX Hello prep failed: could not enable cluster: {}",
            e.message
        ))
    })?;

    switch
        .enable_grpc_for_external_clients(NMX_CONTROLLER_APP, true)
        .await
        .map_err(|e| {
            RmsError::internal(format!(
                "NMX Hello prep failed: could not enable {NMX_CONTROLLER_APP} gRPC: {}",
                e.message
            ))
        })?;

    let status = switch
        .check_grpc_status(NMX_CONTROLLER_APP, switch_host)
        .await
        .map_err(|e| {
            RmsError::internal(format!(
                "NMX Hello prep failed: could not check gRPC status: {}",
                e.message
            ))
        })?;

    let summary = status.get("summary");
    let ready = summary
        .and_then(|s| s.get("ready_for_external_clients"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if ready {
        return Ok(());
    }

    let grpc_enabled = summary
        .and_then(|s| s.get("grpc_enabled"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let port_reachable = summary
        .and_then(|s| s.get("port_reachable"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let nmx_port = grpc_port_for_app(NMX_CONTROLLER_APP);

    Err(RmsError::internal(format!(
        "NMX Hello prep failed: {NMX_CONTROLLER_APP} is not ready for external clients \
         (grpc_enabled={grpc_enabled}, port_{nmx_port}_reachable={port_reachable}). \
         Verify cluster state, that gRPC external access is enabled, and that port {nmx_port} \
         accepts mTLS from --client-tls-root material."
    )))
}

async fn test_nmx_hello(switch_host: &str, client_tls: &NmxcTlsConfig) -> Result<()> {
    if switch_host.is_empty() {
        return Err(RmsError::invalid_argument("switch_host must not be empty"));
    }
    let target_uri = nmx_hello_target_uri(switch_host, true);
    let tls_authority = client_tls.authority.as_deref().unwrap_or("(endpoint host)");
    let gateway_id = gateway_id_from_env();
    tracing::info!(
        switch_host = %switch_host,
        target_uri = %target_uri,
        tls_authority = %tls_authority,
        gateway_id = %gateway_id,
        "starting NMX Hello mTLS connectivity test"
    );
    let endpoint = Endpoint::new(&target_uri).map_err(|e| RmsError::internal(e.to_string()))?;
    let pool = NmxcClientPool::builder()
        .build()
        .map_err(|e| RmsError::internal(e.to_string()))?;
    let mut client = pool
        .create_client(endpoint, Some(client_tls))
        .await
        .map_err(|e| {
            tracing::warn!(
                switch_host = %switch_host,
                target_uri = %target_uri,
                tls_authority = %tls_authority,
                error = %e,
                "NMX Hello connect failed"
            );
            RmsError::internal(format!(
                "NMX Hello connect failed for {target_uri} (authority={tls_authority}): {e}"
            ))
        })?;
    client.hello(&gateway_id).await.map_err(|e| {
        tracing::warn!(
            switch_host = %switch_host,
            target_uri = %target_uri,
            tls_authority = %tls_authority,
            gateway_id = %gateway_id,
            error = %e,
            "NMX Hello RPC failed"
        );
        RmsError::internal(format!(
            "NMX Hello failed for {target_uri} (authority={tls_authority}): {e}"
        ))
    })?;
    tracing::info!(
        switch_host = %switch_host,
        target_uri = %target_uri,
        gateway_id = %gateway_id,
        "NMX Hello mTLS connectivity test succeeded"
    );
    Ok(())
}

async fn test_nmx_hello_with_retries(
    switch: &SwitchGb200Nvidia,
    switch_host: &str,
    client_tls: &NmxcTlsConfig,
) -> Result<()> {
    prepare_nmx_controller_for_hello(switch, switch_host).await?;

    let mut last_err = None;
    for attempt in 0..NMX_HELLO_CONNECT_ATTEMPTS {
        match test_nmx_hello(switch_host, client_tls).await {
            Ok(()) => return Ok(()),
            Err(e) if nmx_hello_connect_error(&e) && attempt + 1 < NMX_HELLO_CONNECT_ATTEMPTS => {
                tracing::info!(
                    switch_host = %switch_host,
                    attempt = attempt + 1,
                    max_attempts = NMX_HELLO_CONNECT_ATTEMPTS,
                    retry_in_secs = NMX_HELLO_CONNECT_RETRY_WAIT.as_secs(),
                    error = %e.message,
                    "NMX Hello connect failed; retrying after mTLS bind"
                );
                last_err = Some(e);
                tokio::time::sleep(NMX_HELLO_CONNECT_RETRY_WAIT).await;
            }
            Err(e) => return Err(e),
        }
    }

    Err(last_err.unwrap_or_else(|| RmsError::internal("NMX Hello connect failed after retries")))
}

async fn test_gnmi_capabilities(
    switch: &SwitchGb200Nvidia,
    switch_host: &str,
    client_tls: &GnmiTlsConfig,
) -> Result<()> {
    if switch_host.is_empty() {
        return Err(RmsError::invalid_argument("switch_host must not be empty"));
    }
    let target_uri = capabilities_target_uri(switch_host);
    let tls_authority = client_tls.authority.as_deref().unwrap_or("(endpoint host)");
    tracing::info!(
        switch_host = %switch_host,
        target_uri = %target_uri,
        tls_authority = %tls_authority,
        "starting gNMI Capabilities mTLS connectivity test"
    );

    let endpoint = GnmiEndpoint::new(&target_uri).map_err(|e| RmsError::internal(e.to_string()))?;
    let pool = GnmiClientPool::builder()
        .build()
        .map_err(|e| RmsError::internal(e.to_string()))?;
    let (username, password) = switch.host_credentials()?;
    let credentials = GnmiCredentials::new(username, password);
    let mut client = pool
        .create_client(endpoint, Some(client_tls), credentials)
        .await
        .map_err(|e| {
            tracing::warn!(
                switch_host = %switch_host,
                target_uri = %target_uri,
                tls_authority = %tls_authority,
                error = %e,
                "gNMI Capabilities connect failed"
            );
            RmsError::internal(capabilities_connect_error_message(
                &target_uri,
                tls_authority,
                e,
            ))
        })?;

    Gnmi::capabilities(&mut *client).await.map_err(|e| {
        tracing::warn!(
            switch_host = %switch_host,
            target_uri = %target_uri,
            tls_authority = %tls_authority,
            error = %e,
            "gNMI Capabilities RPC failed"
        );
        RmsError::internal(capabilities_rpc_error_message(
            &target_uri,
            tls_authority,
            e,
        ))
    })?;

    tracing::info!(
        switch_host = %switch_host,
        target_uri = %target_uri,
        "gNMI Capabilities mTLS connectivity test succeeded"
    );
    Ok(())
}

async fn test_gnmi_capabilities_with_retries(
    switch: &SwitchGb200Nvidia,
    switch_host: &str,
    client_tls: &GnmiTlsConfig,
) -> Result<()> {
    let mut last_err = None;
    for attempt in 0..GNMI_CAPABILITIES_CONNECT_ATTEMPTS {
        match test_gnmi_capabilities(switch, switch_host, client_tls).await {
            Ok(()) => return Ok(()),
            Err(e)
                if is_capabilities_connect_error(&e)
                    && attempt + 1 < GNMI_CAPABILITIES_CONNECT_ATTEMPTS =>
            {
                tracing::info!(
                    switch_host = %switch_host,
                    attempt = attempt + 1,
                    max_attempts = GNMI_CAPABILITIES_CONNECT_ATTEMPTS,
                    retry_in_secs = GNMI_CAPABILITIES_CONNECT_RETRY_WAIT.as_secs(),
                    error = %e.message,
                    "gNMI Capabilities connect failed; retrying after mTLS bind"
                );
                last_err = Some(e);
                tokio::time::sleep(GNMI_CAPABILITIES_CONNECT_RETRY_WAIT).await;
            }
            Err(e) => return Err(e),
        }
    }

    Err(last_err
        .unwrap_or_else(|| RmsError::internal("gNMI Capabilities connect failed after retries")))
}

fn bind_service_stage_name(service: SwitchMtlsService) -> String {
    format!("bind_{}", service.as_str())
}

#[allow(clippy::too_many_arguments)]
fn build_switch_certificate_result_json(
    status: &str,
    domain: &str,
    ca_cert_id: &str,
    entity_cert_id: &str,
    remote_dir: &str,
    configured_services: &[&str],
    timeline: &StageTimeline,
    extra: Value,
) -> String {
    let mut result = json!({
        "status": status,
        "domain": domain,
        "ca_certificate_id": ca_cert_id,
        "entity_certificate_id": entity_cert_id,
        "remote_dir": remote_dir,
        "services": configured_services,
        "timing_summary": timeline.to_json(),
    });
    if let Value::Object(extra_fields) = extra {
        if let Value::Object(ref mut result_fields) = result {
            result_fields.extend(extra_fields);
        }
    }
    result.to_string()
}

fn build_switch_mtls_unset_result_json(
    status: &str,
    services: &[&str],
    timeline: &StageTimeline,
    extra: Value,
) -> String {
    let mut result = json!({
        "status": status,
        "operation": "unset_mtls",
        "mode": "insecure-switch",
        "services": services,
        "timing_summary": timeline.to_json(),
    });

    if let Value::Object(extra_fields) = extra {
        if let Value::Object(ref mut result_fields) = result {
            result_fields.extend(extra_fields);
        }
    }

    result.to_string()
}

fn timeline_stage_summary(timeline: &StageTimeline) -> Value {
    let stages = timeline.to_json()["stages"]
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .map(|entry| {
                    json!({
                        "name": entry.get("name"),
                        "status": entry.get("status"),
                        "duration_ms": entry.get("duration_ms"),
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    json!({
        "current_stage": timeline.to_json()["current_stage"],
        "total_duration_ms": timeline.to_json()["total_duration_ms"],
        "stages": stages,
    })
}

fn debug_switch_certificate_stage_invoke(
    job_id: &str,
    stage: &str,
    operation: &str,
    switch: &SwitchGb200Nvidia,
) {
    tracing::debug!(
        job_id = %job_id,
        node = %switch.id(),
        rack = %switch.rack_id(),
        stage,
        operation,
        "switch certificate stage invoking node operation"
    );
}

fn begin_switch_certificate_stage(
    job_tracker: &JobTracker,
    timeline: &mut StageTimeline,
    job_id: &str,
    stage: &str,
    description: &str,
    switch: &SwitchGb200Nvidia,
) {
    timeline.start(stage, description);
    job_tracker.mark_running(job_id, description);
    tracing::info!(
        job_id = %job_id,
        node = %switch.id(),
        rack = %switch.rack_id(),
        stage,
        description,
        "switch certificate job stage starting"
    );
    tracing::debug!(
        job_id = %job_id,
        node = %switch.id(),
        rack = %switch.rack_id(),
        stage,
        description,
        timeline = ?timeline_stage_summary(timeline),
        "switch certificate job stage entered"
    );
}

fn complete_switch_certificate_stage(
    timeline: &mut StageTimeline,
    job_id: &str,
    stage: &str,
    message: &str,
    details: Value,
    switch: &SwitchGb200Nvidia,
) {
    timeline.complete(stage, message, details.clone());
    tracing::info!(
        job_id = %job_id,
        node = %switch.id(),
        rack = %switch.rack_id(),
        stage,
        message,
        "switch certificate job stage completed"
    );
    tracing::debug!(
        job_id = %job_id,
        node = %switch.id(),
        rack = %switch.rack_id(),
        stage,
        message,
        ?details,
        timeline = ?timeline_stage_summary(timeline),
        "switch certificate job stage finished successfully"
    );
}

fn fail_switch_certificate_stage(
    timeline: &mut StageTimeline,
    job_id: &str,
    stage: &str,
    error: &str,
    details: Value,
    switch: &SwitchGb200Nvidia,
) {
    timeline.fail(stage, error, details.clone());
    tracing::warn!(
        job_id = %job_id,
        node = %switch.id(),
        rack = %switch.rack_id(),
        stage,
        error,
        "switch certificate job stage failed"
    );
    tracing::debug!(
        job_id = %job_id,
        node = %switch.id(),
        rack = %switch.rack_id(),
        stage,
        error,
        ?details,
        timeline = ?timeline_stage_summary(timeline),
        "switch certificate job stage recorded failure"
    );
}

fn finalize_switch_certificate_job_failure(
    job_tracker: &JobTracker,
    job_id: &str,
    failed_stage: &str,
    error: &str,
    result_json: String,
    switch: &SwitchGb200Nvidia,
    timeline: &StageTimeline,
) {
    tracing::debug!(
        job_id = %job_id,
        node = %switch.id(),
        rack = %switch.rack_id(),
        failed_stage,
        error,
        result_bytes = result_json.len(),
        timeline = ?timeline_stage_summary(timeline),
        "switch certificate job terminating after stage failure"
    );
    job_tracker.mark_failed_with_result(job_id, JobError::Other, error, Some(result_json));
}

async fn run_switch_mtls_unset_job(
    job_tracker: Arc<JobTracker>,
    switch: Arc<SwitchGb200Nvidia>,
    services: Vec<SwitchMtlsService>,
    job_id: String,
) {
    let mut timeline = StageTimeline::new();
    let requested_services = services
        .iter()
        .map(|service| service.as_str())
        .collect::<Vec<_>>();

    let stage = STAGE_UNSET_MTLS_MODE;

    tracing::info!(
        job_id = %job_id,
        node = %switch.id(),
        rack = %switch.rack_id(),
        ?services,
        "switch mTLS unset job starting"
    );

    begin_switch_certificate_stage(
        &job_tracker,
        &mut timeline,
        &job_id,
        stage,
        "Unsetting switch service mTLS mode via SSH",
        &switch,
    );

    debug_switch_certificate_stage_invoke(&job_id, stage, "unset_mtls_services", &switch);

    let commands = match switch.unset_mtls_services(&services).await {
        Ok(commands) => commands,
        Err(e) => {
            fail_switch_certificate_stage(
                &mut timeline,
                &job_id,
                stage,
                &e.message,
                json!({ "error": e.message, "services": requested_services.clone() }),
                &switch,
            );

            let result_json = build_switch_mtls_unset_result_json(
                "failed",
                &requested_services,
                &timeline,
                Value::Null,
            );

            finalize_switch_certificate_job_failure(
                &job_tracker,
                &job_id,
                stage,
                &e.message,
                result_json,
                &switch,
                &timeline,
            );

            return;
        }
    };

    complete_switch_certificate_stage(
        &mut timeline,
        &job_id,
        stage,
        "Switch service mTLS mode unset via SSH",
        json!({ "services": requested_services.clone(), "commands": commands.clone() }),
        &switch,
    );

    let result_json = build_switch_mtls_unset_result_json(
        "completed",
        &requested_services,
        &timeline,
        json!({ "commands": commands.clone() }),
    );

    tracing::info!(
        job_id = %job_id,
        node = %switch.id(),
        rack = %switch.rack_id(),
        result_bytes = result_json.len(),
        "switch mTLS unset job completed"
    );

    job_tracker.mark_completed(&job_id, Some(result_json));
}

fn dispatch_switch_mtls_unset_job(
    job_tracker: Arc<JobTracker>,
    switch: Arc<SwitchGb200Nvidia>,
    services: Vec<SwitchMtlsService>,
    job_id: String,
) {
    #[cfg(not(test))]
    {
        tokio::spawn(async move {
            run_switch_mtls_unset_job(job_tracker, switch, services, job_id).await;
        });
    }

    #[cfg(test)]
    {
        let _ = switch;
        let _ = services;

        // Handler unit tests verify request routing and job creation without
        // opening real SSH sessions. The job runner itself is covered with a
        // fake SSH executor.
        job_tracker.mark_running(
            &job_id,
            "switch mTLS unset job dispatch skipped in unit test",
        );
    }
}

// Per-node orchestrator for ConfigureSwitchCertificate.
// Stages: prepare_remote_dir, sftp_copy_<file>, import_material, bind_<service>,
// test_nvue_hello, test_nmx_hello, test_gnmi_capabilities.
#[allow(clippy::too_many_arguments)]
async fn run_switch_certificate_job(
    job_tracker: Arc<JobTracker>,
    switch: Arc<SwitchGb200Nvidia>,
    material: SwitchMtlsMaterialPaths,
    client_tls: Option<StableNmxcTlsConfig>,
    nvue_transition: Option<NvueTlsTransition>,
    services: Vec<SwitchMtlsService>,
    test_hello: bool,
    switch_host: String,
    switch_host_port: u16,
    job_id: String,
) {
    let mut timeline = StageTimeline::new();
    let domain = material.domain.clone();
    let ca_cert_id = material.ca_cert_id();
    let entity_cert_id = material.entity_cert_id();
    let remote_dir = material.remote_dir();
    let requires_external_client_tls = test_hello
        && services.iter().any(|service| {
            matches!(
                service,
                SwitchMtlsService::ScaleUpFabricManager
                    | SwitchMtlsService::ScaleUpFabricTelemetryInterface
            )
        });
    let external_client_tls = client_tls.as_ref().map(StableNmxcTlsConfig::as_config);

    if requires_external_client_tls && external_client_tls.is_none() {
        job_tracker.mark_failed(
            &job_id,
            JobError::ClientError,
            "post-install NMX/gNMI test requires RMS client TLS material",
        );

        return;
    }

    tracing::info!(
        job_id = %job_id,
        node = %switch.id(),
        rack = %switch.rack_id(),
        switch_host = %switch_host,
        switch_host_port,
        domain = %domain,
        remote_dir = %remote_dir,
        ca_cert_id = %ca_cert_id,
        entity_cert_id = %entity_cert_id,
        test_hello,
        ?services,
        "switch certificate configuration job starting"
    );
    tracing::debug!(
        job_id = %job_id,
        node = %switch.id(),
        rack = %switch.rack_id(),
        switch_host = %switch_host,
        switch_host_port,
        domain = %domain,
        remote_dir = %remote_dir,
        ca_cert_id = %ca_cert_id,
        entity_cert_id = %entity_cert_id,
        test_hello,
        ?services,
        "switch certificate configuration job initialized"
    );

    // ── Stage: prepare_remote_dir ──
    const STAGE_PREPARE_REMOTE_DIR: &str = "prepare_remote_dir";
    const STAGE_PREPARE_REMOTE_DIR_DESC: &str =
        "Preparing remote TLS directories on switch via SSH";
    begin_switch_certificate_stage(
        &job_tracker,
        &mut timeline,
        &job_id,
        STAGE_PREPARE_REMOTE_DIR,
        STAGE_PREPARE_REMOTE_DIR_DESC,
        &switch,
    );
    debug_switch_certificate_stage_invoke(
        &job_id,
        STAGE_PREPARE_REMOTE_DIR,
        "prepare_mtls_remote_dirs",
        &switch,
    );
    if let Err(e) = switch.prepare_mtls_remote_dirs(&material).await {
        fail_switch_certificate_stage(
            &mut timeline,
            &job_id,
            STAGE_PREPARE_REMOTE_DIR,
            &e.message,
            json!({ "error": e.message }),
            &switch,
        );
        let result_json = build_switch_certificate_result_json(
            "failed",
            &domain,
            &ca_cert_id,
            &entity_cert_id,
            &remote_dir,
            &[],
            &timeline,
            Value::Null,
        );
        finalize_switch_certificate_job_failure(
            &job_tracker,
            &job_id,
            STAGE_PREPARE_REMOTE_DIR,
            &e.message,
            result_json,
            &switch,
            &timeline,
        );
        return;
    }
    complete_switch_certificate_stage(
        &mut timeline,
        &job_id,
        STAGE_PREPARE_REMOTE_DIR,
        "Remote TLS directories prepared on switch",
        json!({ "remote_dir": remote_dir }),
        &switch,
    );

    // ── Stage: sftp_copy_<file> (one per TLS material file) ──
    for spec in mtls_remote_file_specs(&material) {
        let stage = sftp_copy_stage_name(spec.remote_name);
        let description = format!(
            "Copying TLS material file {} to switch via SFTP",
            spec.remote_name
        );
        begin_switch_certificate_stage(
            &job_tracker,
            &mut timeline,
            &job_id,
            &stage,
            &description,
            &switch,
        );
        debug_switch_certificate_stage_invoke(&job_id, &stage, "sftp_copy_mtls_file", &switch);
        match switch
            .sftp_copy_mtls_file(&material, spec.remote_name)
            .await
        {
            Ok(outcome) => {
                complete_switch_certificate_stage(
                    &mut timeline,
                    &job_id,
                    &stage,
                    &format!("TLS material file {} copied via SFTP", spec.remote_name),
                    json!({
                        "remote_file": spec.remote_name,
                        "remote_path": outcome.remote_path,
                        "bytes": outcome.bytes,
                    }),
                    &switch,
                );
            }
            Err(e) => {
                fail_switch_certificate_stage(
                    &mut timeline,
                    &job_id,
                    &stage,
                    &e.message,
                    json!({
                        "error": e.message,
                        "remote_file": spec.remote_name,
                        "local_path": spec.local_path.display().to_string(),
                    }),
                    &switch,
                );
                let result_json = build_switch_certificate_result_json(
                    "failed",
                    &domain,
                    &ca_cert_id,
                    &entity_cert_id,
                    &remote_dir,
                    &[],
                    &timeline,
                    Value::Null,
                );
                finalize_switch_certificate_job_failure(
                    &job_tracker,
                    &job_id,
                    &stage,
                    &e.message,
                    result_json,
                    &switch,
                    &timeline,
                );
                return;
            }
        }
    }

    // ── Stage: import_material ──
    const STAGE_IMPORT: &str = "import_material";
    const STAGE_IMPORT_DESC: &str = "Importing TLS certificates into NVOS security store";
    begin_switch_certificate_stage(
        &job_tracker,
        &mut timeline,
        &job_id,
        STAGE_IMPORT,
        STAGE_IMPORT_DESC,
        &switch,
    );
    debug_switch_certificate_stage_invoke(&job_id, STAGE_IMPORT, "import_mtls_material", &switch);
    if let Err(e) = switch.import_mtls_material(&material).await {
        fail_switch_certificate_stage(
            &mut timeline,
            &job_id,
            STAGE_IMPORT,
            &e.message,
            json!({ "error": e.message }),
            &switch,
        );
        let result_json = build_switch_certificate_result_json(
            "failed",
            &domain,
            &ca_cert_id,
            &entity_cert_id,
            &remote_dir,
            &[],
            &timeline,
            Value::Null,
        );
        finalize_switch_certificate_job_failure(
            &job_tracker,
            &job_id,
            STAGE_IMPORT,
            &e.message,
            result_json,
            &switch,
            &timeline,
        );
        return;
    }
    complete_switch_certificate_stage(
        &mut timeline,
        &job_id,
        STAGE_IMPORT,
        "TLS certificates imported into NVOS security store",
        json!({
            "ca_certificate_id": ca_cert_id,
            "entity_certificate_id": entity_cert_id,
        }),
        &switch,
    );

    // ── Stage: bind_<service> (one per requested service) ──
    let mut configured = Vec::with_capacity(services.len());
    let ordered_services = services
        .iter()
        .filter(|service| **service != SwitchMtlsService::NvueApi)
        .chain(
            services
                .iter()
                .filter(|service| **service == SwitchMtlsService::NvueApi),
        );

    for service in ordered_services {
        let stage = bind_service_stage_name(*service);
        let description = format!("Binding mTLS to switch service {}", service.as_str());
        begin_switch_certificate_stage(
            &job_tracker,
            &mut timeline,
            &job_id,
            &stage,
            &description,
            &switch,
        );
        debug_switch_certificate_stage_invoke(&job_id, &stage, "bind_mtls_service", &switch);
        let bind_result = if *service == SwitchMtlsService::NvueApi {
            match nvue_transition.as_ref() {
                Some(transition) => {
                    bind_nvue_mtls_with_client_transition(&switch, &material, transition).await
                }
                None => Err(RmsError::failed_precondition(
                    "NVUE mTLS requires RMS client TLS material",
                )),
            }
        } else {
            switch.bind_mtls_service(&material, *service).await
        };

        if let Err(e) = bind_result {
            fail_switch_certificate_stage(
                &mut timeline,
                &job_id,
                &stage,
                &e.message,
                json!({
                    "error": e.message,
                    "service": service.as_str(),
                }),
                &switch,
            );
            let result_json = build_switch_certificate_result_json(
                "failed",
                &domain,
                &ca_cert_id,
                &entity_cert_id,
                &remote_dir,
                &configured,
                &timeline,
                Value::Null,
            );
            finalize_switch_certificate_job_failure(
                &job_tracker,
                &job_id,
                &stage,
                &e.message,
                result_json,
                &switch,
                &timeline,
            );
            return;
        }
        configured.push(service.as_str());
        complete_switch_certificate_stage(
            &mut timeline,
            &job_id,
            &stage,
            &format!("mTLS bound to {}", service.as_str()),
            json!({ "service": service.as_str() }),
            &switch,
        );
    }

    const STAGE_VERIFY_SERVICES: &str = "verify_services";
    begin_switch_certificate_stage(
        &job_tracker,
        &mut timeline,
        &job_id,
        STAGE_VERIFY_SERVICES,
        "Verifying switch service certificate configuration",
        &switch,
    );
    debug_switch_certificate_stage_invoke(
        &job_id,
        STAGE_VERIFY_SERVICES,
        "verify_mtls_services",
        &switch,
    );
    let service_verification = match switch.verify_mtls_services(&material, &services).await {
        Ok(verification) => {
            complete_switch_certificate_stage(
                &mut timeline,
                &job_id,
                STAGE_VERIFY_SERVICES,
                "Switch service certificate configuration verified",
                verification.clone(),
                &switch,
            );
            verification
        }
        Err(e) => {
            fail_switch_certificate_stage(
                &mut timeline,
                &job_id,
                STAGE_VERIFY_SERVICES,
                &e.message,
                json!({ "error": e.message }),
                &switch,
            );
            let result_json = build_switch_certificate_result_json(
                "failed",
                &domain,
                &ca_cert_id,
                &entity_cert_id,
                &remote_dir,
                &configured,
                &timeline,
                Value::Null,
            );
            finalize_switch_certificate_job_failure(
                &job_tracker,
                &job_id,
                STAGE_VERIFY_SERVICES,
                &e.message,
                result_json,
                &switch,
                &timeline,
            );
            return;
        }
    };

    let mut extra = serde_json::Map::new();
    extra.insert("service_verification".into(), service_verification);

    if test_hello {
        if services.contains(&SwitchMtlsService::NvueApi) {
            const STAGE_NVUE_HELLO: &str = "test_nvue_hello";
            const STAGE_NVUE_HELLO_DESC: &str =
                "Running post-install NVUE API mTLS connectivity test";
            begin_switch_certificate_stage(
                &job_tracker,
                &mut timeline,
                &job_id,
                STAGE_NVUE_HELLO,
                STAGE_NVUE_HELLO_DESC,
                &switch,
            );
            debug_switch_certificate_stage_invoke(
                &job_id,
                STAGE_NVUE_HELLO,
                "test_nvue_hello_with_retries",
                &switch,
            );

            if let Err(e) = test_nvue_hello_with_retries(&switch).await {
                fail_switch_certificate_stage(
                    &mut timeline,
                    &job_id,
                    STAGE_NVUE_HELLO,
                    &e.message,
                    json!({
                        "error": e.message,
                        "switch_host": switch_host,
                        "switch_host_port": switch_host_port,
                    }),
                    &switch,
                );
                let result_json = build_switch_certificate_result_json(
                    "failed",
                    &domain,
                    &ca_cert_id,
                    &entity_cert_id,
                    &remote_dir,
                    &configured,
                    &timeline,
                    Value::Null,
                );
                finalize_switch_certificate_job_failure(
                    &job_tracker,
                    &job_id,
                    STAGE_NVUE_HELLO,
                    &e.message,
                    result_json,
                    &switch,
                    &timeline,
                );
                return;
            }
            complete_switch_certificate_stage(
                &mut timeline,
                &job_id,
                STAGE_NVUE_HELLO,
                "NVUE API mTLS connectivity test succeeded",
                json!({
                    "switch_host": switch_host,
                    "switch_host_port": switch_host_port,
                }),
                &switch,
            );
            extra.insert("nvue_hello_test".into(), json!("success"));
        }

        if let (true, Some(client_tls)) = (
            services.contains(&SwitchMtlsService::ScaleUpFabricManager),
            external_client_tls,
        ) {
            const STAGE_NMX_HELLO: &str = "test_nmx_hello";
            const STAGE_NMX_HELLO_DESC: &str = "Running post-install NMX Hello connectivity test";
            let tls_authority = client_tls.authority.as_deref().unwrap_or("(unknown)");
            begin_switch_certificate_stage(
                &job_tracker,
                &mut timeline,
                &job_id,
                STAGE_NMX_HELLO,
                STAGE_NMX_HELLO_DESC,
                &switch,
            );
            debug_switch_certificate_stage_invoke(
                &job_id,
                STAGE_NMX_HELLO,
                "test_nmx_hello_with_retries",
                &switch,
            );

            if let Err(e) = test_nmx_hello_with_retries(&switch, &switch_host, client_tls).await {
                fail_switch_certificate_stage(
                    &mut timeline,
                    &job_id,
                    STAGE_NMX_HELLO,
                    &e.message,
                    json!({
                        "error": e.message,
                        "switch_host": switch_host,
                        "tls_authority": tls_authority,
                    }),
                    &switch,
                );
                let result_json = build_switch_certificate_result_json(
                    "failed",
                    &domain,
                    &ca_cert_id,
                    &entity_cert_id,
                    &remote_dir,
                    &configured,
                    &timeline,
                    Value::Null,
                );
                finalize_switch_certificate_job_failure(
                    &job_tracker,
                    &job_id,
                    STAGE_NMX_HELLO,
                    &e.message,
                    result_json,
                    &switch,
                    &timeline,
                );
                return;
            }
            complete_switch_certificate_stage(
                &mut timeline,
                &job_id,
                STAGE_NMX_HELLO,
                "NMX Hello connectivity test succeeded",
                json!({
                    "switch_host": switch_host,
                    "tls_authority": tls_authority,
                }),
                &switch,
            );
            extra.insert("hello_test".into(), json!("success"));
        }

        if let (true, Some(client_tls)) = (
            services.contains(&SwitchMtlsService::ScaleUpFabricTelemetryInterface),
            external_client_tls,
        ) {
            const STAGE_GNMI_CAPABILITIES: &str = "test_gnmi_capabilities";
            const STAGE_GNMI_CAPABILITIES_DESC: &str =
                "Running post-install gNMI Capabilities mTLS connectivity test";

            begin_switch_certificate_stage(
                &job_tracker,
                &mut timeline,
                &job_id,
                STAGE_GNMI_CAPABILITIES,
                STAGE_GNMI_CAPABILITIES_DESC,
                &switch,
            );
            debug_switch_certificate_stage_invoke(
                &job_id,
                STAGE_GNMI_CAPABILITIES,
                "test_gnmi_capabilities_with_retries",
                &switch,
            );
            if let Err(e) =
                test_gnmi_capabilities_with_retries(&switch, &switch_host, client_tls).await
            {
                fail_switch_certificate_stage(
                    &mut timeline,
                    &job_id,
                    STAGE_GNMI_CAPABILITIES,
                    &e.message,
                    json!({
                        "error": e.message,
                        "switch_host": switch_host,
                        "tls_authority": client_tls.authority.as_deref(),
                    }),
                    &switch,
                );
                let result_json = build_switch_certificate_result_json(
                    "failed",
                    &domain,
                    &ca_cert_id,
                    &entity_cert_id,
                    &remote_dir,
                    &configured,
                    &timeline,
                    Value::Null,
                );
                finalize_switch_certificate_job_failure(
                    &job_tracker,
                    &job_id,
                    STAGE_GNMI_CAPABILITIES,
                    &e.message,
                    result_json,
                    &switch,
                    &timeline,
                );
                return;
            }
            complete_switch_certificate_stage(
                &mut timeline,
                &job_id,
                STAGE_GNMI_CAPABILITIES,
                "gNMI Capabilities mTLS connectivity test succeeded",
                json!({
                    "switch_host": switch_host,
                    "tls_authority": client_tls.authority.as_deref(),
                }),
                &switch,
            );
            extra.insert("gnmi_capabilities_test".into(), json!("success"));
        }
    }

    let result_json = build_switch_certificate_result_json(
        "completed",
        &domain,
        &ca_cert_id,
        &entity_cert_id,
        &remote_dir,
        &configured,
        &timeline,
        Value::Object(extra),
    );
    tracing::info!(
        job_id = %job_id,
        node = %switch.id(),
        rack = %switch.rack_id(),
        domain = %domain,
        test_hello,
        configured_services = ?configured,
        result_bytes = result_json.len(),
        "switch certificate configuration job completed"
    );
    tracing::debug!(
        job_id = %job_id,
        node = %switch.id(),
        rack = %switch.rack_id(),
        domain = %domain,
        test_hello,
        configured_services = ?configured,
        result_bytes = result_json.len(),
        timeline = ?timeline_stage_summary(&timeline),
        "switch certificate configuration job persisting successful result"
    );
    job_tracker.mark_completed(&job_id, Some(result_json));
}

impl RackManagerServiceImpl {
    pub(crate) async fn handle_configure_switch_certificate(
        &self,
        req: tonic::Request<rm::ConfigureSwitchCertificateRequest>,
    ) -> std::result::Result<tonic::Response<rm::ConfigureSwitchCertificateResponse>, tonic::Status>
    {
        let r = req.into_inner();
        tracing::info!(
            domain = ?r.domain,
            test_hello = r.test_hello,
            service_count = r.services.len(),
            node_count = r.nodes.as_ref().map(|ns| ns.nodes.len()).unwrap_or(0),
            "ConfigureSwitchCertificate request received"
        );

        // Insecure mode must not resolve NVLink domains or certificate roots.
        // Missing or invalid --default-switch-domain, --switch-cert-root,
        // --client-tls-root, and --dns-domain are intentionally ignored here.
        let secure_domain = if self.switch_tls_roots.insecure_switch {
            None
        } else {
            match self.switch_tls_roots.effective_domain(r.domain.as_deref()) {
                Ok(domain) => Some(domain),
                Err(message) => {
                    tracing::warn!(
                        domain = ?r.domain,
                        error = %message,
                        "ConfigureSwitchCertificate domain resolution failed"
                    );

                    return Err(tonic::Status::failed_precondition(message));
                }
            }
        };

        let services = match parse_switch_services(&r.services) {
            Ok(services) => services,
            Err(message) => {
                tracing::warn!(
                    domain = secure_domain.as_deref().unwrap_or("(insecure-switch)"),
                    service_count = r.services.len(),
                    error = %message,
                    "ConfigureSwitchCertificate invalid services"
                );
                return Err(tonic::Status::invalid_argument(message));
            }
        };

        // Hello checks only make sense after installing secure service modes.
        // In insecure mode the request becomes an SSH unset operation, so
        // test_hello is ignored instead of forcing TLS material resolution.
        if let Some(domain) = secure_domain.as_deref() {
            if let Err(message) = validate_test_hello_services(r.test_hello, &services) {
                tracing::warn!(
                    domain,
                    test_hello = r.test_hello,
                    ?services,
                    error = %message,
                    "ConfigureSwitchCertificate invalid test_hello services"
                );

                return Err(tonic::Status::invalid_argument(message));
            }
        } else if r.test_hello {
            tracing::warn!(
                test_hello = r.test_hello,
                ?services,
                "ConfigureSwitchCertificate test_hello ignored in insecure switch mode"
            );
        }

        let mode = if let Some(domain) = secure_domain {
            // Secure install path resolves all certificate material once before
            // per-node work starts, so bad local TLS configuration fails the RPC
            // before any asynchronous job is created.
            let tls_material = match self
                .switch_tls_roots
                .resolve_switch_cert(r.domain.as_deref())
            {
                Ok(material) => material,
                Err(message) => {
                    tracing::warn!(
                        domain = %domain,
                        error = %message,
                        "ConfigureSwitchCertificate switch TLS material resolution failed"
                    );

                    return Err(tonic::Status::failed_precondition(message));
                }
            };

            let nvue_client_tls = if services.contains(&SwitchMtlsService::NvueApi) {
                let Some(tls) = self
                    .switch_tls_roots
                    .resolve_nvue_client_tls(r.domain.as_deref())
                    .map_err(tonic::Status::failed_precondition)?
                else {
                    return Err(tonic::Status::failed_precondition(
                        "NVUE mTLS installation requires RMS client TLS material",
                    ));
                };

                Some(tls)
            } else {
                None
            };

            let needs_external_hello_tls = r.test_hello
                && services.iter().any(|service| {
                    matches!(
                        service,
                        SwitchMtlsService::ScaleUpFabricManager
                            | SwitchMtlsService::ScaleUpFabricTelemetryInterface
                    )
                });

            let client_tls = needs_external_hello_tls
                .then(|| {
                    self.switch_tls_roots
                        .resolve_client_tls(r.domain.as_deref())
                })
                .transpose()
                .map_err(tonic::Status::failed_precondition)?;

            let client_tls_authority = client_tls
                .as_ref()
                .and_then(|client_tls| client_tls.authority.as_deref());

            tracing::info!(
                domain = %domain,
                test_hello = r.test_hello,
                ?services,
                client_tls_authority = client_tls_authority.unwrap_or("(not configured)"),
                switch_ca_cert_path = tls_material
                    .ca_cert_path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_default(),
                switch_client_cert_path = tls_material
                    .client_cert_path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_default(),
                "ConfigureSwitchCertificate resolved TLS material"
            );

            let (Some(ca_cert_path), Some(entity_cert_path), Some(entity_key_path)) = (
                tls_material.ca_cert_path,
                tls_material.client_cert_path,
                tls_material.client_key_path,
            ) else {
                return Err(tonic::Status::failed_precondition(
                    "switch certificate material requires ca.pem, client.pem, and client.key",
                ));
            };

            let material = SwitchMtlsMaterialPaths::load_stable(
                domain.clone(),
                ca_cert_path,
                entity_cert_path,
                entity_key_path,
            )
            .await
            .map_err(|error| tonic::Status::failed_precondition(error.message))?;

            let client_tls = match client_tls {
                Some(config) => Some(
                    StableNmxcTlsConfig::load(config)
                        .await
                        .map_err(|error| tonic::Status::failed_precondition(error.message))?,
                ),
                None => None,
            };

            SwitchCertificateMode::Install(Box::new(SwitchCertificateInstallMode {
                domain,
                material,
                client_tls,
                nvue_client_tls,
            }))
        } else {
            // --insecure-switch changes ConfigureSwitchCertificate into a
            // cleanup workflow: no cert files are read and per-node jobs unset
            // service mTLS modes through SSH.
            SwitchCertificateMode::UnsetMtls
        };

        let devices = r.nodes.map(|ns| ns.nodes).unwrap_or_default();
        if devices.is_empty() {
            tracing::warn!(
                domain = mode.domain_for_log(),
                "ConfigureSwitchCertificate rejected: nodes is empty"
            );
            return Err(tonic::Status::invalid_argument(
                "nodes is required and must contain at least one device",
            ));
        }

        let total_nodes = devices.len() as u32;
        let mut batch = rm::NodeBatchResponse {
            status: rm::ReturnCode::Failure.into(),
            message: String::new(),
            node_results: Vec::new(),
            job_id: String::new(),
            stats: None,
        };
        let mut jobs = Vec::new();
        let mut child_ids = Vec::new();
        let mut parent_rack_id = None;
        let mut skipped = 0u32;

        for device in devices {
            let node_id = device.node_id.clone();
            let rack_id = device.rack_id.clone();
            let node_type_key = device.r#type.unwrap_or(0);
            let node_type = proto_node_type_to_domain(node_type_key);

            tracing::info!(
                node = %node_id,
                rack = %rack_id,
                domain = mode.domain_for_log(),
                node_type = node_type_key,
                "processing switch certificate configuration request"
            );

            if !node_type.is_some_and(|node_type| node_type.kind() == NodeKind::Switch) {
                tracing::warn!(
                    node = %node_id,
                    rack = %rack_id,
                    node_type = node_type_key,
                    "skipping switch certificate configuration for non-switch node"
                );
                batch.node_results.push(rm::NodeOperationResult {
                    node_id: node_id.clone(),
                    status: rm::ReturnCode::Failure.into(),
                    error_message: format!(
                        "device {} is not a switch (type={})",
                        device.node_id,
                        proto_node_type_to_string(node_type_key).unwrap_or("unknown")
                    ),
                });
                skipped += 1;
                continue;
            }

            let flat = match flatten_node_info(&device) {
                Ok(flat) => flat,
                Err(e) => {
                    tracing::error!(
                        node = %node_id,
                        rack = %rack_id,
                        error = %e.message,
                        "invalid endpoint configuration for switch certificate configuration"
                    );
                    batch.node_results.push(rm::NodeOperationResult {
                        node_id: node_id.clone(),
                        status: rm::ReturnCode::Failure.into(),
                        error_message: e.message,
                    });
                    skipped += 1;
                    continue;
                }
            };

            let Some(node_type) = node_type else {
                continue;
            };

            if flat.creds_for_node_type(node_type).is_none() {
                tracing::error!(
                    node = %node_id,
                    rack = %rack_id,
                    "missing host credentials for switch certificate configuration"
                );
                batch.node_results.push(rm::NodeOperationResult {
                    node_id: node_id.clone(),
                    status: rm::ReturnCode::Failure.into(),
                    error_message: format!(
                        "Missing host credentials for switch {}",
                        device.node_id
                    ),
                });
                skipped += 1;
                continue;
            }

            let bmc_endpoint = match flat.optional_bmc_endpoint() {
                Ok(endpoint) => endpoint,
                Err(e) => {
                    tracing::warn!(
                        node = %node_id,
                        rack = %rack_id,
                        error = %e.message,
                        "ignoring malformed BMC endpoint for switch certificate configuration"
                    );
                    None
                }
            };
            let host_endpoint = match flat.switch_host_management_endpoint() {
                Ok(endpoint) => endpoint,
                Err(e) => {
                    tracing::error!(
                        node = %node_id,
                        rack = %rack_id,
                        error = %e.message,
                        "invalid host endpoint for switch certificate configuration"
                    );
                    batch.node_results.push(rm::NodeOperationResult {
                        node_id: node_id.clone(),
                        status: rm::ReturnCode::Failure.into(),
                        error_message: format!(
                            "Invalid host endpoint for switch {}: {}",
                            device.node_id, e.message
                        ),
                    });
                    skipped += 1;
                    continue;
                }
            };

            let switch_host = host_endpoint.endpoint.ip_address.clone();
            let switch_host_port = host_endpoint.endpoint.port;
            let endpoint_authority = host_endpoint
                .endpoint
                .host_name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .unwrap_or(&switch_host);

            let tls_server_name = if mode.is_unset_mtls() {
                // Unset jobs do not make outbound TLS/NVUE requests, but switch
                // construction still needs a stable endpoint label for logging.
                endpoint_authority.to_owned()
            } else {
                match self
                    .switch_tls_roots
                    .tls_server_name_for_endpoint(endpoint_authority, r.domain.as_deref())
                {
                    Ok(server_name) => server_name,
                    Err(message) => {
                        batch.node_results.push(rm::NodeOperationResult {
                            node_id: node_id.clone(),
                            status: rm::ReturnCode::Failure.into(),
                            error_message: message,
                        });

                        skipped += 1;
                        continue;
                    }
                }
            };

            let nvue_server_name = tls_server_name.clone();

            let mut node_client_tls = match &mode {
                SwitchCertificateMode::Install(install) => install.client_tls.clone(),
                SwitchCertificateMode::UnsetMtls => None,
            };

            if let Some(client_tls) = node_client_tls.as_mut() {
                client_tls.set_authority_if_none(&tls_server_name);
            }

            let config = NodeConfig {
                id: node_id.clone(),
                node_type,
                bmc_endpoint,
                host_endpoint: Some(host_endpoint),
            };

            let switch = match node_type {
                DomainNodeType::SwitchGb200Nvidia => {
                    SwitchGb200Nvidia::from_config(&config, &rack_id)
                }
                DomainNodeType::SwitchGb300Nvidia => {
                    SwitchGb300Nvidia::from_config(&config, &rack_id)
                }
                _ => Err(RmsError::unimplemented(
                    "configure switch certificate",
                    node_type,
                )),
            };

            let switch = if mode.is_unset_mtls() {
                // Insecure cleanup does not use NVUE, so avoid registered NVUE
                // client validation and all client TLS state.
                switch
            } else {
                let registered_nvue = self
                    .rack_manager
                    .find_rack(&rack_id)
                    .and_then(|rack| rack.find_node(&node_id))
                    .and_then(|node| node.nvue_client().cloned());

                switch.and_then(|switch| match registered_nvue {
                    Some(client) => {
                        let (username, password) = switch.host_credentials()?;

                        match use_registered_nvue_client(
                            client.credentials_match(username, password),
                            services.contains(&SwitchMtlsService::NvueApi),
                        )? {
                            true => switch.with_nvue_client(client),
                            false => {
                                switch.validate_nvue_client_endpoint(&client)?;

                                tracing::debug!(
                                    node = %node_id,
                                    rack = %rack_id,
                                    "using request-local NVUE client because registered credentials differ"
                                );

                                Ok(switch)
                            }
                        }
                    }
                    None => Ok(switch),
                })
            };

            let switch = match switch {
                Ok(switch) => Arc::new(switch),
                Err(error) => {
                    tracing::error!(
                        node = %node_id,
                        rack = %rack_id,
                        switch_host = %switch_host,
                        switch_host_port,
                        error = %error.message,
                        "failed to construct switch for certificate configuration"
                    );
                    batch.node_results.push(rm::NodeOperationResult {
                        node_id: node_id.clone(),
                        status: rm::ReturnCode::Failure.into(),
                        error_message: error.message,
                    });
                    skipped += 1;
                    continue;
                }
            };

            let nvue_transition = match &mode {
                SwitchCertificateMode::Install(install) => {
                    // Secure NVUE binding may temporarily switch the client
                    // between old and new TLS material while certificates are
                    // installed and verified.
                    if let Err(error) = self
                        .ensure_nvue_client_for_certificate(
                            switch.optional_nvue_client(),
                            r.domain.as_deref(),
                            Some(&nvue_server_name),
                        )
                        .await
                    {
                        batch.node_results.push(rm::NodeOperationResult {
                            node_id: node_id.clone(),
                            status: rm::ReturnCode::Failure.into(),
                            error_message: error.message,
                        });

                        skipped += 1;
                        continue;
                    }

                    match prepare_nvue_tls_transition(
                        &switch,
                        install.nvue_client_tls.as_ref(),
                        nvue_server_name,
                    )
                    .await
                    {
                        Ok(transition) => transition,
                        Err(error) => {
                            batch.node_results.push(rm::NodeOperationResult {
                                node_id: node_id.clone(),
                                status: rm::ReturnCode::Failure.into(),
                                error_message: error.message,
                            });

                            skipped += 1;
                            continue;
                        }
                    }
                }
                SwitchCertificateMode::UnsetMtls => None,
            };

            let job_id = match self.job_tracker.create_job_if_node_idle(
                &rack_id,
                &node_id,
                JobType::SwitchCertificate,
            ) {
                Ok(job_id) => job_id,
                Err(e) => {
                    tracing::error!(
                        node = %node_id,
                        rack = %rack_id,
                        error = %e.message,
                        "failed to create switch certificate job"
                    );
                    batch.node_results.push(rm::NodeOperationResult {
                        node_id: node_id.clone(),
                        status: rm::ReturnCode::Failure.into(),
                        error_message: format!("Failed to create job: {}", e.message),
                    });
                    skipped += 1;
                    continue;
                }
            };
            jobs.push(rm::ConfigureSwitchCertificateJobInfo {
                node_id: node_id.clone(),
                job_id: job_id.clone(),
            });
            child_ids.push(job_id.clone());
            if parent_rack_id.is_none() {
                parent_rack_id = Some(rack_id.clone());
            }

            let services_spawn = services.clone();

            tracing::info!(
                job_id = %job_id,
                node = %node_id,
                rack = %rack_id,
                switch_host = %switch_host,
                switch_host_port,
                domain = mode.domain_for_log(),
                test_hello = r.test_hello,
                ?services,
                mode = mode.job_label(),
                "dispatching switch certificate configuration job"
            );

            match &mode {
                SwitchCertificateMode::Install(install) => {
                    let job_tracker = self.job_tracker.clone();
                    let material_spawn = install.material.clone();
                    let client_tls_spawn = node_client_tls;
                    let switch_host_spawn = switch_host.clone();
                    let test_hello = r.test_hello;

                    tokio::spawn(async move {
                        run_switch_certificate_job(
                            job_tracker,
                            switch,
                            material_spawn,
                            client_tls_spawn,
                            nvue_transition,
                            services_spawn,
                            test_hello,
                            switch_host_spawn,
                            switch_host_port,
                            job_id,
                        )
                        .await;
                    });
                }
                SwitchCertificateMode::UnsetMtls => {
                    let job_tracker = self.job_tracker.clone();

                    dispatch_switch_mtls_unset_job(job_tracker, switch, services_spawn, job_id);
                }
            }
        }

        let jobs_created = child_ids.len() as u32;
        if jobs_created == 0 {
            tracing::warn!(
                domain = mode.domain_for_log(),
                total_nodes,
                skipped,
                test_hello = r.test_hello,
                ?services,
                "ConfigureSwitchCertificate created no jobs"
            );

            batch.message = format!("No {} jobs created", mode.job_label());

            batch.stats = Some(rm::NodeOperationStats {
                total_nodes,
                successful_nodes: 0,
                failed_nodes: skipped,
            });
            return Ok(tonic::Response::new(
                rm::ConfigureSwitchCertificateResponse {
                    response: Some(batch),
                    jobs,
                },
            ));
        }

        let parent_id = self
            .job_tracker
            .create_parent_job(
                parent_rack_id.as_deref().unwrap_or("batch"),
                &child_ids,
                JobType::SwitchCertificate,
            )
            .unwrap_or_default();
        batch.job_id = parent_id;
        batch.status = if skipped == 0 {
            rm::ReturnCode::Success
        } else {
            rm::ReturnCode::Failure
        }
        .into();

        batch.message = format!(
            "Created {jobs_created} {job_label} jobs out of {total_nodes} nodes. \
             Use GetConfigureSwitchCertificateJobStatus with job_id to track progress.",
            job_label = mode.job_label()
        );
        batch.stats = Some(rm::NodeOperationStats {
            total_nodes,
            successful_nodes: jobs_created,
            failed_nodes: skipped,
        });

        tracing::info!(
            parent_job_id = %batch.job_id,
            parent_rack_id = parent_rack_id.as_deref().unwrap_or("batch"),
            jobs_created,
            total_nodes,
            skipped,
            domain = mode.domain_for_log(),
            test_hello = r.test_hello,
            ?services,
            mode = mode.job_label(),
            "ConfigureSwitchCertificate jobs dispatched"
        );

        Ok(tonic::Response::new(
            rm::ConfigureSwitchCertificateResponse {
                response: Some(batch),
                jobs,
            },
        ))
    }

    pub(crate) async fn handle_get_configure_switch_certificate_job_status(
        &self,
        req: tonic::Request<rm::GetConfigureSwitchCertificateJobStatusRequest>,
    ) -> std::result::Result<
        tonic::Response<rm::GetConfigureSwitchCertificateJobStatusResponse>,
        tonic::Status,
    > {
        let r = req.into_inner();
        tracing::info!(
            job_id = %r.job_id,
            "GetConfigureSwitchCertificateJobStatus request received"
        );
        let mut resp = rm::GetConfigureSwitchCertificateJobStatusResponse {
            status: rm::ReturnCode::Failure.into(),
            job_id: r.job_id.clone(),
            state: String::new(),
            message: String::new(),
            rack_id: String::new(),
            node_id: String::new(),
            error_message: String::new(),
            result_json: String::new(),
            created_at: None,
            updated_at: None,
        };

        if r.job_id.is_empty() {
            tracing::warn!("GetConfigureSwitchCertificateJobStatus rejected: job_id is empty");
            resp.message = "job_id is required".into();
            return Ok(tonic::Response::new(resp));
        }

        let Some(info) = self.job_tracker.get_job(&r.job_id) else {
            tracing::warn!(
                job_id = %r.job_id,
                "GetConfigureSwitchCertificateJobStatus job not found"
            );
            resp.message = format!("job {} not found", r.job_id);
            return Ok(tonic::Response::new(resp));
        };

        tracing::debug!(
            job_id = %info.job_id,
            node = %info.node_id,
            rack = %info.rack_id,
            state = %info.state.as_str(),
            has_error = !info.error_message.is_empty(),
            "GetConfigureSwitchCertificateJobStatus returning job status"
        );

        resp.status = rm::ReturnCode::Success.into();
        resp.job_id = info.job_id;
        resp.state = info.state.as_str().to_owned();
        resp.message = info.state_description;
        resp.rack_id = info.rack_id;
        resp.node_id = info.node_id;
        resp.error_message = info.error_message;
        resp.result_json = info.result_json;
        resp.created_at = Some(timestamp_from_datetime(info.created_at));
        resp.updated_at = Some(timestamp_from_datetime(info.updated_at));
        Ok(tonic::Response::new(resp))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::api::grpc::server::SwitchTlsRoots;
    use crate::orchestrator::job_tracker::JobState;
    use crate::orchestrator::rack_manager::RackManager;
    use crate::persistence::Backends;
    use crate::utilities::error::ErrorCode;

    fn test_service(switch_tls_roots: SwitchTlsRoots) -> RackManagerServiceImpl {
        RackManagerServiceImpl {
            rack_manager: Arc::new(RackManager::new()),
            job_tracker: Arc::new(JobTracker::new()),
            firmware_download_cancellations: Arc::new(Mutex::new(HashMap::new())),
            backends: Backends::memory(),
            firmware_dir: PathBuf::from("firmware"),
            switch_tls_roots,
            sftp_upload_options: crate::transport::ssh::SftpUploadOptions::default(),
        }
    }

    fn switch_node_info() -> rm::NodeInfo {
        rm::NodeInfo {
            node_id: "sw-01".into(),
            rack_id: "rack-01".into(),
            r#type: Some(rm::NodeType::SwitchGb200Nvidia as i32),
            bmc_endpoint: None,
            host_endpoint: Some(rm::Endpoint {
                interface: Some(rm::NetworkInterface {
                    ip_address: "192.0.2.10".into(),
                    mac_address: String::new(),
                    host_name: Some("switch.example.com".into()),
                }),
                port: 443,
                credentials: Some(rm::Credentials {
                    auth: Some(rm::credentials::Auth::UserPass(rm::UsernamePassword {
                        username: "admin".into(),
                        password: "password".into(),
                    })),
                }),
                dangerously_accept_invalid_certs: false,
            }),
        }
    }

    #[test]
    fn sftp_copy_stage_name_uses_remote_file_label() {
        assert_eq!(sftp_copy_stage_name("ca.pem"), "sftp_copy_ca_pem");
        assert_eq!(sftp_copy_stage_name("client.key"), "sftp_copy_client_key");
    }

    #[test]
    fn bind_service_stage_name_uses_service_label() {
        assert_eq!(
            bind_service_stage_name(SwitchMtlsService::NvueApi),
            "bind_nvue-api"
        );
    }

    #[test]
    fn build_switch_certificate_result_json_includes_timing_summary() {
        let timeline = StageTimeline::new();
        let json = build_switch_certificate_result_json(
            "completed",
            "fabric-a",
            "rms-fabric-a-ca",
            "rms-fabric-a-cert",
            "/home/admin/certs/run",
            &["nvue-api"],
            &timeline,
            json!({ "nvue_hello_test": "success" }),
        );
        let parsed: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["status"], "completed");
        assert_eq!(parsed["domain"], "fabric-a");
        assert_eq!(parsed["services"][0], "nvue-api");
        assert_eq!(parsed["nvue_hello_test"], "success");
        assert!(parsed["timing_summary"]["stages"].is_array());
    }

    #[test]
    fn build_switch_mtls_unset_result_json_marks_insecure_operation() {
        let timeline = StageTimeline::new();

        let json = build_switch_mtls_unset_result_json(
            "completed",
            &["nvue-api"],
            &timeline,
            json!({ "commands": ["nv unset system api mtls ca-certificate"] }),
        );

        let parsed: Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["operation"], "unset_mtls");
        assert_eq!(parsed["mode"], "insecure-switch");
        assert_eq!(parsed["services"][0], "nvue-api");
    }

    #[tokio::test]
    async fn run_switch_mtls_unset_job_completes_with_fake_ssh() {
        let tracker = Arc::new(JobTracker::new());
        let job_id = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchCertificate)
            .expect("create switch certificate job");

        let switch = Arc::new(
            SwitchGb200Nvidia::for_test("http://127.0.0.1")
                .with_ssh_exec_for_test(|_| Ok(String::new())),
        );

        run_switch_mtls_unset_job(
            Arc::clone(&tracker),
            switch,
            vec![SwitchMtlsService::NvueApi],
            job_id.clone(),
        )
        .await;

        let info = tracker.get_job(&job_id).unwrap();
        let result: Value = serde_json::from_str(&info.result_json).unwrap();

        assert_eq!(info.state, JobState::Completed);
        assert_eq!(result["operation"], "unset_mtls");

        assert_eq!(
            result["commands"][0],
            "nv unset system api mtls ca-certificate"
        );
    }

    #[tokio::test]
    async fn configure_switch_certificate_insecure_mode_skips_tls_material() {
        for dns_domain in [None, Some("bad host")] {
            let service = test_service(SwitchTlsRoots {
                default_domain: Some("../bad".into()),
                dns_domain: dns_domain.map(str::to_owned),
                insecure_switch: true,
                ..SwitchTlsRoots::default()
            });

            let response = service
                .handle_configure_switch_certificate(tonic::Request::new(
                    rm::ConfigureSwitchCertificateRequest {
                        domain: None,
                        services: vec![rm::SwitchService::NvueApi as i32],
                        nodes: Some(rm::NodeSet {
                            nodes: vec![switch_node_info()],
                        }),
                        test_hello: true,
                    },
                ))
                .await
                .unwrap()
                .into_inner();

            let batch = response.response.unwrap();

            assert_eq!(batch.status, rm::ReturnCode::Success as i32);
            assert_eq!(batch.node_results.len(), 0);
            assert_eq!(response.jobs.len(), 1);
            assert!(batch.message.contains("switch mTLS unset"));
        }
    }

    #[test]
    fn parse_switch_services_rejects_unspecified() {
        let err = parse_switch_services(&[rm::SwitchService::Unspecified as i32]).unwrap_err();
        assert!(err.contains("UNSPECIFIED"));
    }

    #[test]
    fn parse_switch_services_deduplicates() {
        let services = parse_switch_services(&[
            rm::SwitchService::NvueApi as i32,
            rm::SwitchService::NvueApi as i32,
        ])
        .unwrap();
        assert_eq!(services.len(), 1);
    }

    #[test]
    fn nvue_bind_rejects_request_credentials_that_bypass_registered_client() {
        assert!(use_registered_nvue_client(true, true).unwrap());
        assert!(!use_registered_nvue_client(false, false).unwrap());
        assert!(use_registered_nvue_client(false, true).is_err());
    }

    #[test]
    fn validate_test_hello_services_matches_supported_services() {
        for (test_hello, raw_services, expected_ok) in [
            (
                true,
                vec![rm::SwitchService::ScaleUpFabricTelemetry as i32],
                false,
            ),
            (true, vec![rm::SwitchService::NvueApi as i32], true),
            (false, vec![rm::SwitchService::NvueApi as i32], true),
            (
                true,
                vec![
                    rm::SwitchService::NvueApi as i32,
                    rm::SwitchService::ScaleUpFabricManager as i32,
                ],
                true,
            ),
            (
                true,
                vec![rm::SwitchService::ScaleUpFabricTelemetryInterface as i32],
                true,
            ),
        ] {
            let services = parse_switch_services(&raw_services).unwrap();

            assert_eq!(
                validate_test_hello_services(test_hello, &services).is_ok(),
                expected_ok
            );
        }
    }

    #[test]
    fn nmx_hello_target_uri_uses_hostname() {
        assert_eq!(
            nmx_hello_target_uri("switch.example.com", true),
            format!(
                "https://switch.example.com:{}",
                grpc_port_for_app(NMX_CONTROLLER_APP)
            )
        );
    }

    #[test]
    fn nmx_hello_target_uri_formats_ipv6_literal() {
        assert_eq!(
            nmx_hello_target_uri("2001:db8::1", true),
            format!(
                "https://[2001:db8::1]:{}",
                grpc_port_for_app(NMX_CONTROLLER_APP)
            )
        );
    }

    #[test]
    fn nvue_hello_connect_error_detects_connectivity_failures() {
        let err = RmsError::internal(
            "NVUE GET /nvue_v1/system failed: POST /nvue_v1/revision: tls handshake failure",
        );

        assert!(nvue_hello_connect_error(&err));

        let auth_err = RmsError::new(
            ErrorCode::Unauthenticated,
            "HTTP GET /nvue_v1/system returned 401",
        );

        assert!(nvue_hello_connect_error(&auth_err));

        let logic_err = RmsError::internal("NVUE action job-1 failed with state action_error");

        assert!(!nvue_hello_connect_error(&logic_err));
    }

    #[test]
    fn nmx_hello_connect_error_detects_transport_failures() {
        let err = RmsError::internal(
            "NMX Hello connect failed for https://10.0.0.1:9370 (authority=switch.example.com): \
             Transport error: transport error",
        );
        assert!(nmx_hello_connect_error(&err));
        let rpc_err = RmsError::internal("NMX Hello failed for https://10.0.0.1:9370: gRPC status");
        assert!(!nmx_hello_connect_error(&rpc_err));
    }
}
