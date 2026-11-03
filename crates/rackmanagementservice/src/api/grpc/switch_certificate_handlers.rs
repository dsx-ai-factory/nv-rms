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

//! Handlers for ConfigureSwitchCertificate and GetConfigureSwitchCertificateJobStatus.
//! BatchDisableSwitchMtls uses the existing switch mTLS unset workflow.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use librms::protos::rack_manager as rm;
use serde::Serialize;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use super::scaleupfabricmanager_handlers::{
    disable_insecure_nmx_controller_mtls, nmx_client_tls_with_endpoint_authority,
    wait_for_nmx_controller_ready_for_config,
};
use super::server::RackManagerServiceImpl;
use crate::api::grpc::conversions::{
    batch_targets, failed_node_results, flatten_node_info, proto_node_type_to_domain,
    proto_node_type_to_string, timestamp_from_datetime,
};
use crate::domain::node::{NodeKind, NodeType as DomainNodeType};
use crate::domain::rack::NodeConfig;
use crate::libgnmi::{
    Endpoint as GnmiEndpoint, Gnmi, GnmiClientPool, GnmiCredentials, GnmiTlsConfig,
    capabilities_connect_error_message, capabilities_rpc_error_message, capabilities_target_uri,
    is_capabilities_connect_error,
};
use crate::libnmxc::{Endpoint, NmxcClientPool, NmxcTlsConfig};
use crate::nodes::SwitchScaleUpManagement;
use crate::nodes::nvos::GnmiClientCa;
use crate::nodes::switch_gb200_nvidia::{SwitchGb200Nvidia, grpc_port_for_app};
use crate::nodes::switch_gb300_nvidia::SwitchGb300Nvidia;
use crate::nodes::switch_mtls::{
    CertificateBindings, CertificateInventory, CertificateStore, GNMI_CLIENT_AUTHENTICATION_MTLS,
    GNMI_CLIENT_AUTHENTICATION_SERVER_CERTIFICATE_ONLY, MtlsSftpCopyOutcome, MtlsUnsetReport,
    MtlsUnsetStage, NVOS_CERTIFICATE_STORE_LIMIT, StableNmxcTlsConfig, SwitchMtlsMaterialPaths,
    SwitchMtlsService, mtls_remote_file_specs, plan_store_reclaim, sftp_copy_stage_name,
};
#[cfg(test)]
use crate::nodes::switch_mtls::{
    MtlsPersistOutcome, MtlsServiceOutcome, MtlsUnsetFailureSite, MtlsUnsetStatus,
};
use crate::nodes::switch_vrnvl72_nvidia::SwitchVrnvl72Nvidia;
use crate::orchestrator::job_lifecycle::{JobError, JobFailure, JobJoinHandle};
use crate::orchestrator::job_tracker::JobTracker;
use crate::orchestrator::job_tracker::{JobType, RmsJobHandle};
use crate::orchestrator::stage_timeline::{Stage, StageTimeline, fail_job};
use crate::utilities::error::{ErrorCode, Result, RmsError};
use crate::utilities::url::grpc_target_uri;

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

/// Predefined, single-stage sequence driving `run_switch_mtls_unset_job`.
fn switch_mtls_unset_job_stages() -> Vec<Stage> {
    vec![Stage::new(
        "unset_mtls_mode".to_owned(),
        "Unset switch service mTLS mode via SSH".to_owned(),
    )]
}

/// Builds the `StageTimeline` driving `run_switch_mtls_unset_job`, tagged
/// with the switch's node/rack so `StageTimeline`'s own stage transition
/// logs carry them without repeating them at every call site. Factored out
/// of `run_switch_mtls_unset_job` so this wiring -- easy to silently drop
/// when refactoring -- has a direct unit test.
fn switch_mtls_unset_job_timeline(job_id: &str, switch: &SwitchGb200Nvidia) -> StageTimeline {
    StageTimeline::with_stages(job_id, switch_mtls_unset_job_stages())
        .with_node_rack(switch.id(), switch.rack_id())
}

/// Orders `services` so `NvueApi`'s mTLS bind happens last. Binding mTLS
/// on every other service is itself performed over the current (possibly
/// still non-mTLS) NVUE client; flipping NVUE's own mTLS mode is what
/// actually starts enforcing client certs on that connection, so doing it
/// last keeps the client usable for every other service's bind. Shared by
/// `switch_certificate_job_stages` (which plans the predefined stage
/// list) and `run_switch_certificate_job` (which performs the binds) so
/// the two can never drift out of order with each other.
fn ordered_bind_services(
    services: &[SwitchMtlsService],
) -> impl Iterator<Item = &SwitchMtlsService> {
    services
        .iter()
        .filter(|service| **service != SwitchMtlsService::NvueApi)
        .chain(
            services
                .iter()
                .filter(|service| **service == SwitchMtlsService::NvueApi),
        )
}

/// Predefined, ordered stage sequence driving `run_switch_certificate_job`.
/// Built once, up front, from the same install-specific inputs (which TLS
/// material files need to be copied, which services need to be bound, and
/// whether/what post-install connectivity tests will run) that drive the
/// loops in the job body, so the timeline's cursor advances in lockstep with
/// the work actually performed.
fn switch_certificate_job_stages(
    material: &SwitchMtlsMaterialPaths,
    services: &[SwitchMtlsService],
    test_hello: bool,
    has_external_client_tls: bool,
) -> Vec<Stage> {
    let stage = |name: String, description: String| Stage::new(name, description);

    let mut stages = vec![
        stage(
            "reclaim_certificate_capacity".to_owned(),
            "Make room in the NVOS certificate stores".to_owned(),
        ),
        stage(
            "prepare_remote_dir".to_owned(),
            "Prepare remote TLS directories on switch via SSH".to_owned(),
        ),
    ];

    for spec in mtls_remote_file_specs(material) {
        stages.push(stage(
            sftp_copy_stage_name(spec.remote_name),
            format!(
                "Copy TLS material file {} to switch via SFTP",
                spec.remote_name
            ),
        ));
    }

    stages.push(stage(
        "import_material".to_owned(),
        "Import TLS certificates into NVOS security store".to_owned(),
    ));

    if certificate_job_discovers_nvue_capabilities(services) {
        stages.push(stage(
            "discover_nvue_capabilities".to_owned(),
            "Discover switch capabilities from live NVUE".to_owned(),
        ));
    }

    for service in ordered_bind_services(services) {
        stages.push(stage(
            bind_service_stage_name(*service),
            format!("Bind mTLS to switch service {}", service.as_str()),
        ));
    }

    stages.push(stage(
        "verify_services".to_owned(),
        "Verify switch service certificate configuration".to_owned(),
    ));

    if test_hello {
        if services.contains(&SwitchMtlsService::NvueApi) {
            stages.push(stage(
                "test_nvue_hello".to_owned(),
                "Run post-install NVUE API mTLS connectivity test".to_owned(),
            ));
        }
        if has_external_client_tls && services.contains(&SwitchMtlsService::ScaleUpFabricManager) {
            stages.push(stage(
                "test_nmx_hello".to_owned(),
                "Run post-install NMX Hello connectivity test".to_owned(),
            ));
        }
        if has_external_client_tls
            && services.contains(&SwitchMtlsService::ScaleUpFabricTelemetryInterface)
        {
            stages.push(stage(
                "test_gnmi_capabilities".to_owned(),
                "Run post-install gNMI Capabilities mTLS connectivity test".to_owned(),
            ));
        }
    }

    stages
}

/// Builds the `StageTimeline` driving `run_switch_certificate_job`. See
/// `switch_mtls_unset_job_timeline`.
#[allow(clippy::too_many_arguments)]
fn switch_certificate_job_timeline(
    job_id: &str,
    material: &SwitchMtlsMaterialPaths,
    services: &[SwitchMtlsService],
    test_hello: bool,
    has_external_client_tls: bool,
    switch: &SwitchGb200Nvidia,
) -> StageTimeline {
    StageTimeline::with_stages(
        job_id,
        switch_certificate_job_stages(material, services, test_hello, has_external_client_tls),
    )
    .with_node_rack(switch.id(), switch.rack_id())
}

#[derive(Clone, Copy, Serialize)]
enum SwitchMtlsUnsetMode {
    #[serde(rename = "disable-mtls")]
    DisableMtls,

    #[serde(rename = "insecure-switch")]
    InsecureSwitch,
}

/// Verified client state prepared before the switch enables NVUE mTLS.
///
/// The registered client stays isolated during bootstrap and joins the
/// transition only after the switch starts enforcing the new transport.
#[derive(Clone)]
pub(crate) struct NvueTlsTransition {
    tls: nvue_client::ClientTls,
    server_name: String,
    fingerprint: [u8; 32],
    registered_client: Option<nvue_client::SharedClient>,
}

/// A certificate-install phase reported to the caller-owned progress sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SwitchCertificateInstallStage {
    /// Make room for the import in the NVOS certificate stores.
    ReclaimCertificateCapacity,

    /// Prepare the switch-side staging directory.
    PrepareRemoteDirectory,

    /// Transfer one certificate material file.
    TransferMaterial(&'static str),

    /// Import the staged material into the NVOS security store.
    ImportMaterial,

    /// Classify live NVUE capabilities before a gNMI bind.
    DiscoverNvueCapabilities,

    /// Bind the imported material to one switch service.
    BindService(SwitchMtlsService),

    /// Verify every requested service binding.
    VerifyServices,

    /// Test the verified NVUE API transport.
    TestNvueHello,

    /// Test the verified NMX transport.
    TestNmxHello,

    /// Test the verified gNMI transport.
    TestGnmiCapabilities,
}

impl SwitchCertificateInstallStage {
    pub(crate) fn name(self) -> String {
        match self {
            Self::ReclaimCertificateCapacity => "reclaim_certificate_capacity".to_owned(),
            Self::PrepareRemoteDirectory => "prepare_remote_dir".to_owned(),
            Self::TransferMaterial(remote_name) => sftp_copy_stage_name(remote_name),
            Self::ImportMaterial => "import_material".to_owned(),
            Self::DiscoverNvueCapabilities => "discover_nvue_capabilities".to_owned(),
            Self::BindService(service) => bind_service_stage_name(service),
            Self::VerifyServices => "verify_services".to_owned(),
            Self::TestNvueHello => "test_nvue_hello".to_owned(),
            Self::TestNmxHello => "test_nmx_hello".to_owned(),
            Self::TestGnmiCapabilities => "test_gnmi_capabilities".to_owned(),
        }
    }
}

/// Receives certificate-install stage transitions without owning job state.
///
/// The certificate RPC uses this interface to retain its detailed timeline.
/// Callers that already have a coarser outer stage may use a no-op sink.
pub(crate) trait SwitchCertificateInstallProgress: Send {
    /// Start `stage` before its switch operation runs.
    fn start_stage(&mut self, stage: SwitchCertificateInstallStage) -> Result<()>;

    /// Complete the current stage with its diagnostic details.
    fn complete_stage(&mut self, details: Value);
}

/// Switch operations required by the shared certificate installer.
///
/// Implementations must make cleanup best-effort: cleanup failures must not
/// replace an installation error or turn an otherwise successful install into
/// a failure.
#[async_trait]
pub(crate) trait SwitchCertificateInstallNode: Send + Sync {
    /// Return the node identifier used for cleanup diagnostics.
    fn node_id(&self) -> &str;

    /// List the entries in both NVOS certificate stores.
    async fn read_certificate_inventory(&self) -> Result<CertificateInventory>;

    /// Read which certificate-store entries every switch service is bound to,
    /// including services the request does not bind.
    async fn read_certificate_bindings(&self) -> Result<CertificateBindings>;

    /// Delete one RMS-created entry from an NVOS certificate store.
    async fn delete_certificate_entry(&self, store: CertificateStore, id: &str) -> Result<()>;

    /// Prepare the remote staging directory.
    async fn prepare_mtls_remote_dirs(&self, material: &SwitchMtlsMaterialPaths) -> Result<()>;

    /// Copy one material file into the remote staging directory.
    async fn sftp_copy_mtls_file(
        &self,
        material: &SwitchMtlsMaterialPaths,
        remote_file_name: &str,
        cancel: CancellationToken,
    ) -> Result<MtlsSftpCopyOutcome>;

    /// Import staged certificate material into the NVOS security store.
    async fn import_mtls_material(&self, material: &SwitchMtlsMaterialPaths) -> Result<()>;

    /// Classify live NVUE capabilities before binding a service that depends on them.
    async fn initialize_nvue_capabilities(&self) -> Result<GnmiClientCa>;

    /// Remove staged material without changing the operation outcome.
    async fn cleanup_mtls_remote_dir_best_effort(&self, material: &SwitchMtlsMaterialPaths);

    /// Bind imported material to `service`, including the verified NVUE
    /// transport transition when binding the NVUE API itself.
    async fn bind_mtls_service_for_install(
        &self,
        material: &SwitchMtlsMaterialPaths,
        service: SwitchMtlsService,
        nvue_transition: Option<&NvueTlsTransition>,
    ) -> Result<()>;

    /// Verify every requested service binding.
    async fn verify_mtls_services(
        &self,
        material: &SwitchMtlsMaterialPaths,
        services: &[SwitchMtlsService],
    ) -> Result<Value>;
}

#[async_trait]
impl SwitchCertificateInstallNode for SwitchGb200Nvidia {
    fn node_id(&self) -> &str {
        self.id()
    }

    async fn read_certificate_inventory(&self) -> Result<CertificateInventory> {
        SwitchGb200Nvidia::read_certificate_inventory(self).await
    }

    async fn read_certificate_bindings(&self) -> Result<CertificateBindings> {
        Ok(SwitchGb200Nvidia::read_certificate_bindings(self).await)
    }

    async fn delete_certificate_entry(&self, store: CertificateStore, id: &str) -> Result<()> {
        SwitchGb200Nvidia::delete_certificate_entry(self, store, id).await
    }

    async fn prepare_mtls_remote_dirs(&self, material: &SwitchMtlsMaterialPaths) -> Result<()> {
        SwitchGb200Nvidia::prepare_mtls_remote_dirs(self, material).await
    }

    async fn sftp_copy_mtls_file(
        &self,
        material: &SwitchMtlsMaterialPaths,
        remote_file_name: &str,
        cancel: CancellationToken,
    ) -> Result<MtlsSftpCopyOutcome> {
        SwitchGb200Nvidia::sftp_copy_mtls_file(self, material, remote_file_name, cancel).await
    }

    async fn import_mtls_material(&self, material: &SwitchMtlsMaterialPaths) -> Result<()> {
        SwitchGb200Nvidia::import_mtls_material(self, material).await
    }

    async fn initialize_nvue_capabilities(&self) -> Result<GnmiClientCa> {
        SwitchGb200Nvidia::initialize_nvue_capabilities(self).await
    }

    async fn cleanup_mtls_remote_dir_best_effort(&self, material: &SwitchMtlsMaterialPaths) {
        SwitchGb200Nvidia::cleanup_mtls_remote_dir_best_effort(self, material).await;
    }

    async fn bind_mtls_service_for_install(
        &self,
        material: &SwitchMtlsMaterialPaths,
        service: SwitchMtlsService,
        nvue_transition: Option<&NvueTlsTransition>,
    ) -> Result<()> {
        if service != SwitchMtlsService::NvueApi {
            return SwitchGb200Nvidia::bind_mtls_service(self, material, service).await;
        }

        let Some(transition) = nvue_transition else {
            return Err(RmsError::failed_precondition(
                "NVUE mTLS requires RMS client TLS material",
            ));
        };

        bind_nvue_mtls_with_client_transition(self, material, transition).await
    }

    async fn verify_mtls_services(
        &self,
        material: &SwitchMtlsMaterialPaths,
        services: &[SwitchMtlsService],
    ) -> Result<Value> {
        SwitchGb200Nvidia::verify_mtls_services(self, material, services).await
    }
}

struct SwitchCertificateInstallMode {
    domain: String,
    material: SwitchMtlsMaterialPaths,
    client_tls: Option<StableNmxcTlsConfig>,
    nvue_client_tls: Option<nvue_client::ClientTls>,
}

struct QueuedSwitchMtlsDisableJob {
    job: RmsJobHandle,
    switch: Arc<SwitchGb200Nvidia>,
    node_id: String,
}

/// Per-node state a `ConfigureSwitchCertificate` job needs, built while the
/// node is being admitted to the batch.
struct PreparedSwitchCertificateNode {
    switch: Arc<SwitchGb200Nvidia>,
    client_tls: Option<StableNmxcTlsConfig>,
    nvue_server_name: String,
    registered_nvue_client: Option<nvue_client::SharedClient>,
    switch_host: String,
    switch_host_port: u16,
}

/// One service's outcome as reported to the client.
#[derive(Serialize)]
struct SwitchMtlsUnsetServiceResult<'a> {
    service: &'a str,
    stage: &'static str,
    status: &'static str,
}

/// One persist operation's outcome as reported to the client.
#[derive(Serialize)]
struct SwitchMtlsUnsetPersistResult {
    stage: &'static str,
    status: &'static str,
}

/// Where an unset run stopped. `service` is omitted when apply or save failed,
/// since neither is attributable to a single service.
#[derive(Serialize)]
struct SwitchMtlsUnsetFailure<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    service: Option<&'a str>,
    stage: &'static str,
    message: &'a str,
}

/// The job result for an mTLS unset run.
///
/// Reports what changed per service rather than the `nv` command lines that
/// changed it: command text is an NVOS implementation detail that belongs in
/// RMS logs, not in a client-visible payload.
#[derive(Serialize)]
struct SwitchMtlsUnsetResult<'a> {
    status: &'a str,
    operation: &'static str,
    mode: SwitchMtlsUnsetMode,
    services: &'a [&'a str],
    service_results: Vec<SwitchMtlsUnsetServiceResult<'a>>,
    persist: Vec<SwitchMtlsUnsetPersistResult>,
    timing_summary: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure: Option<SwitchMtlsUnsetFailure<'a>>,
}

enum SwitchCertificateMode {
    // Secure mode installs switch-side certificates and binds the selected
    // services. NVUE and NMX require a client CA. gNMI binds a client CA when
    // the image exposes `/system/gnmi-server/mtls`. A 404 binds the server
    // certificate only, and the completed job records
    // `gnmi_client_authentication` as `server-certificate-only`.
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
    client_role: &'static str,
) -> Result<()> {
    let endpoint = client.endpoint();

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
            Ok(_) => {
                tracing::debug!(
                    client_role,
                    host = %endpoint.host,
                    connect_host = %endpoint.connect_host,
                    server_name = %transition.server_name,
                    attempt = attempt + 1,
                    "verified NVUE client TLS activation succeeded"
                );

                return Ok(());
            }
            Err(nvue_client::ClientError::StalePreparedTransport)
                if attempt + 1 < NVUE_HELLO_CONNECT_ATTEMPTS =>
            {
                tracing::debug!(
                    client_role,
                    host = %endpoint.host,
                    connect_host = %endpoint.connect_host,
                    server_name = %transition.server_name,
                    attempt = attempt + 1,
                    max_attempts = NVUE_HELLO_CONNECT_ATTEMPTS,
                    "prepared NVUE client TLS became stale; rebuilding before retry"
                );

                prepared = client
                    .prepare_verified_client_tls(
                        transition.tls.clone(),
                        Some(&transition.server_name),
                    )
                    .await?;

                if prepared.material_fingerprint() != transition.fingerprint {
                    tracing::warn!(
                        client_role,
                        host = %endpoint.host,
                        connect_host = %endpoint.connect_host,
                        server_name = %transition.server_name,
                        "NVUE client TLS material changed while rebuilding stale transport"
                    );

                    return Err(RmsError::failed_precondition(
                        "RMS NVUE client TLS material changed during certificate installation",
                    ));
                }
            }
            Err(error) => {
                let error = RmsError::from(error);

                if !nvue_hello_connect_error(&error) || attempt + 1 == NVUE_HELLO_CONNECT_ATTEMPTS {
                    tracing::warn!(
                        client_role,
                        host = %endpoint.host,
                        connect_host = %endpoint.connect_host,
                        server_name = %transition.server_name,
                        attempt = attempt + 1,
                        max_attempts = NVUE_HELLO_CONNECT_ATTEMPTS,
                        error = %error.message,
                        "verified NVUE client TLS activation failed"
                    );

                    return Err(error);
                }

                tracing::info!(
                    client_role,
                    host = %endpoint.host,
                    connect_host = %endpoint.connect_host,
                    server_name = %transition.server_name,
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

pub(crate) async fn prepare_nvue_tls_transition(
    client: Option<&nvue_client::SharedClient>,
    tls: Option<&nvue_client::ClientTls>,
    server_name: String,
    registered_client: Option<nvue_client::SharedClient>,
) -> Result<Option<NvueTlsTransition>> {
    let Some(tls) = tls else {
        return Ok(None);
    };

    let client = client.ok_or_else(|| {
        RmsError::failed_precondition("NVUE client is required for the verified TLS transition")
    })?;

    let prepared = client
        .prepare_verified_client_tls(tls.clone(), Some(&server_name))
        .await?;

    Ok(Some(NvueTlsTransition {
        tls: tls.clone(),
        server_name,
        fingerprint: prepared.material_fingerprint(),
        registered_client,
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
        .prepare_verified_client_tls(transition.tls.clone(), Some(&transition.server_name))
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

    // Complete the bind only after both the job-local and registered clients
    // can use the verified mTLS transport.
    let activation = async {
        activate_nvue_client_tls_with_retries(client, prepared, transition, "job-local").await?;

        if let Some(registered_client) = transition.registered_client.as_deref() {
            let registered_prepared = registered_client
                .prepare_verified_client_tls(transition.tls.clone(), Some(&transition.server_name))
                .await?;

            if registered_prepared.material_fingerprint() != transition.fingerprint {
                return Err(RmsError::failed_precondition(
                    "RMS NVUE client TLS material changed during certificate installation",
                ));
            }

            activate_nvue_client_tls_with_retries(
                registered_client,
                registered_prepared,
                transition,
                "registered",
            )
            .await?;
        }

        Ok(())
    }
    .await;

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

    wait_for_nmx_controller_ready_for_config(switch, switch_host)
        .await
        .map_err(|e| {
            RmsError::internal(format!(
                "NMX Hello prep failed: controller did not become ready: {}",
                e.message
            ))
        })
}

async fn test_nmx_hello(
    switch_host: &str,
    client_tls: &NmxcTlsConfig,
    gateway_id: &str,
) -> Result<()> {
    if switch_host.is_empty() {
        return Err(RmsError::invalid_argument("switch_host must not be empty"));
    }
    let target_uri = nmx_hello_target_uri(switch_host, true);
    let tls_authority = client_tls.authority.as_deref().unwrap_or("(endpoint host)");
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
    client.hello(gateway_id).await.map_err(|e| {
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
    gateway_id: &str,
) -> Result<()> {
    prepare_nmx_controller_for_hello(switch, switch_host).await?;

    let mut last_err = None;
    for attempt in 0..NMX_HELLO_CONNECT_ATTEMPTS {
        match test_nmx_hello(switch_host, client_tls, gateway_id).await {
            Ok(()) => return Ok(()),
            Err(e) if nmx_hello_connect_error(&e) && attempt + 1 < NMX_HELLO_CONNECT_ATTEMPTS => {
                tracing::info!(
                    switch_host = %switch_host,
                    attempt = attempt + 1,
                    max_attempts = NMX_HELLO_CONNECT_ATTEMPTS,
                    retry_in_secs = NMX_HELLO_CONNECT_RETRY_WAIT.as_secs(),
                    error = %e.message,
                    "NMX Hello connect failed; retrying"
                );
                last_err = Some(e);
                tokio::time::sleep(NMX_HELLO_CONNECT_RETRY_WAIT).await;
            }
            Err(e) => return Err(e),
        }
    }

    Err(last_err.unwrap_or_else(|| RmsError::internal("NMX Hello connect failed after retries")))
}

/// Reconciles the primary's NMX Controller mTLS binding and checks Hello.
///
/// A stale binding is reconfigured before Hello, even if the superseded
/// certificate still completes a handshake. A current binding is checked with
/// bounded retries for Hello connect failures, without rebinding. When the
/// binding cannot be read, a one-shot Hello connect failure triggers a rebind
/// followed by the same retry check; other Hello failures are returned directly.
async fn ensure_primary_nmx_controller_mtls(
    binding_is_current: impl Future<Output = Result<bool>>,
    check: impl Future<Output = Result<()>>,
    configure: impl Future<Output = Result<()>>,
    check_with_retries: impl Future<Output = Result<()>>,
) -> Result<()> {
    match binding_is_current.await {
        Ok(true) => return check_with_retries.await,
        Ok(false) => {
            tracing::info!(
                "configuring primary NMX Controller mTLS with the current NVUE API certificate material"
            );

            configure.await?;
            return check_with_retries.await;
        }
        Err(error) => {
            tracing::warn!(
                error = %error.message,
                "could not compare NMX Controller mTLS binding with NVUE API material; deciding by connectivity check"
            );
        }
    }

    match check.await {
        Ok(()) => return Ok(()),
        Err(error) if nmx_hello_connect_error(&error) => {
            tracing::info!(
                error = %error.message,
                "configuring primary NMX Controller mTLS after connectivity check failed"
            );
        }
        Err(error) => return Err(error),
    }

    configure.await?;
    check_with_retries.await
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

/// Job-result and discover-stage label for a gNMI client-CA classification.
fn gnmi_client_authentication_label(capabilities: GnmiClientCa) -> &'static str {
    if capabilities.is_supported() {
        GNMI_CLIENT_AUTHENTICATION_MTLS
    } else {
        GNMI_CLIENT_AUTHENTICATION_SERVER_CERTIFICATE_ONLY
    }
}

/// Copy a named gNMI authentication outcome out of service verification.
///
/// `null` means gNMI was not selected. A string is `mtls` or
/// `server-certificate-only` and belongs on the completed job result beside
/// `services`.
fn recorded_gnmi_client_authentication(service_verification: &Value) -> Option<Value> {
    service_verification
        .get("gnmi_client_authentication")
        .filter(|value| value.is_string())
        .cloned()
}

/// gNMI bind/verify classifies the live NVOS image; other services do not.
fn certificate_job_discovers_nvue_capabilities(services: &[SwitchMtlsService]) -> bool {
    services.contains(&SwitchMtlsService::ScaleUpFabricTelemetryInterface)
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
    if let Value::Object(extra_fields) = extra
        && let Value::Object(ref mut result_fields) = result
    {
        result_fields.extend(extra_fields);
    }
    result.to_string()
}

/// The per-service outcomes of `report`, as the result JSON renders them.
fn switch_mtls_unset_service_results(
    report: &MtlsUnsetReport,
) -> Vec<SwitchMtlsUnsetServiceResult<'_>> {
    report
        .services
        .iter()
        .map(|outcome| SwitchMtlsUnsetServiceResult {
            service: outcome.service.as_str(),
            stage: MtlsUnsetStage::UnsetBinding.as_str(),
            status: outcome.status.as_str(),
        })
        .collect()
}

/// The persist outcomes of `report`, as the result JSON renders them.
fn switch_mtls_unset_persist_results(
    report: &MtlsUnsetReport,
) -> Vec<SwitchMtlsUnsetPersistResult> {
    report
        .persist
        .iter()
        .map(|outcome| SwitchMtlsUnsetPersistResult {
            stage: outcome.stage.as_str(),
            status: outcome.status.as_str(),
        })
        .collect()
}

/// Where `report` stopped, as the result JSON renders it.
fn switch_mtls_unset_failure(report: &MtlsUnsetReport) -> Option<SwitchMtlsUnsetFailure<'_>> {
    report.failure.as_ref().map(|site| SwitchMtlsUnsetFailure {
        service: site.service.map(SwitchMtlsService::as_str),
        stage: site.stage.as_str(),
        message: &site.message,
    })
}

fn build_switch_mtls_unset_result<'a>(
    status: &'a str,
    mode: SwitchMtlsUnsetMode,
    services: &'a [&'a str],
    timeline: &StageTimeline,
    report: &'a MtlsUnsetReport,
) -> SwitchMtlsUnsetResult<'a> {
    SwitchMtlsUnsetResult {
        status,
        operation: "unset_mtls",
        mode,
        services,
        service_results: switch_mtls_unset_service_results(report),
        persist: switch_mtls_unset_persist_results(report),
        timing_summary: timeline.to_json(),
        failure: switch_mtls_unset_failure(report),
    }
}

/// The per-service view of `report`, for a stage's `details` field.
///
/// Serializes the same typed values the result JSON uses, rather than
/// rebuilding them, so a stage entry and the top-level result cannot disagree
/// about what happened -- including which keys are present at all, since
/// `failure.service` is omitted rather than null for a persist failure.
fn switch_mtls_unset_stage_details(services: &[&str], report: &MtlsUnsetReport) -> Value {
    let mut details = json!({ "services": services });

    let Value::Object(fields) = &mut details else {
        return details;
    };

    for (key, value) in [
        (
            "service_results",
            serde_json::to_value(switch_mtls_unset_service_results(report)),
        ),
        (
            "persist",
            serde_json::to_value(switch_mtls_unset_persist_results(report)),
        ),
    ] {
        match value {
            Ok(value) => {
                fields.insert(key.to_owned(), value);
            }
            Err(error) => tracing::error!(
                error = %error,
                key,
                "failed to serialize switch mTLS unset stage details"
            ),
        }
    }

    if let Some(failure) = switch_mtls_unset_failure(report) {
        match serde_json::to_value(failure) {
            Ok(value) => {
                fields.insert("failure".to_owned(), value);
            }
            Err(error) => tracing::error!(
                error = %error,
                "failed to serialize switch mTLS unset failure site"
            ),
        }
    }

    details
}

fn serialize_switch_mtls_unset_result(result: &SwitchMtlsUnsetResult<'_>) -> String {
    match serde_json::to_string(result) {
        Ok(result_json) => result_json,
        Err(error) => {
            tracing::error!(
                error = %error,
                "failed to serialize switch mTLS unset result"
            );

            String::new()
        }
    }
}

async fn run_switch_mtls_unset_job(
    job: RmsJobHandle,
    switch: Arc<SwitchGb200Nvidia>,
    services: Vec<SwitchMtlsService>,
    result_mode: SwitchMtlsUnsetMode,
) {
    let job_id = job.id().to_string();
    let mut timeline = switch_mtls_unset_job_timeline(&job_id, &switch);
    let requested_services = services
        .iter()
        .map(|service| service.as_str())
        .collect::<Vec<_>>();

    tracing::info!(
        job_id = %job_id,
        node = %switch.id(),
        rack = %switch.rack_id(),
        ?services,
        "switch mTLS unset job starting"
    );

    // Starts the job's single predefined stage, or fails the job and
    // returns if the predefined stage list and this function's control
    // flow have drifted out of sync -- a programming error, not a runtime
    // condition. Without this, `start_stage()` running past the end of the
    // list would panic through `.expect(...)`; the job supervisor's
    // Drop-based safety net (see `JobHandle`'s `Drop` impl) still catches
    // that and marks the job failed, but only with a generic "job
    // abandoned" message and a noisy panic backtrace.
    let Ok((_stage, description)) = timeline.start_stage() else {
        timeline.skip_remaining();
        let report = MtlsUnsetReport::not_attempted(&services);
        let result = build_switch_mtls_unset_result(
            "failed",
            result_mode,
            &requested_services,
            &timeline,
            &report,
        );
        let result_json = serialize_switch_mtls_unset_result(&result);
        job.fail(
            JobFailure::new(JobError::Other, "stage list exhausted").with_result_json(result_json),
        );
        return;
    };
    job.progress(description);

    // Cancellation is handled inside the flow. Dropping the unset future at
    // an arbitrary await point would discard the report describing how far it
    // got, and `nv action restore` takes effect on the switch immediately, so
    // an abandoned run cannot honestly be described as having attempted
    // nothing.
    let cancellation_token = job.cancellation_token();
    let report = match switch
        .unset_mtls_services(&services, &cancellation_token)
        .await
    {
        Ok(report) => report,
        Err(failure) => {
            // The partial report is the whole point of the failure path: it
            // names which services had already been cleared before the run
            // stopped, and which were never attempted.
            let report = failure.report;
            let error = failure.error;

            timeline.fail_current(
                false,
                &error.message,
                switch_mtls_unset_stage_details(&requested_services, &report),
            );
            timeline.skip_remaining();

            let result = build_switch_mtls_unset_result(
                "failed",
                result_mode,
                &requested_services,
                &timeline,
                &report,
            );

            let result_json = serialize_switch_mtls_unset_result(&result);

            job.fail(
                JobFailure::new(JobError::Other, &error.message).with_result_json(result_json),
            );

            return;
        }
    };

    if report.cancelled {
        let message = "switch mTLS unset job cancelled";

        // The report carries whatever the run got through before it stopped,
        // so a client can see which services were already changed and that
        // config_apply / config_save never ran -- i.e. that any staged change
        // is uncommitted on the switch.
        timeline.fail_current(
            true,
            message,
            switch_mtls_unset_stage_details(&requested_services, &report),
        );
        timeline.skip_remaining();

        let result = build_switch_mtls_unset_result(
            "cancelled",
            result_mode,
            &requested_services,
            &timeline,
            &report,
        );

        let result_json = serialize_switch_mtls_unset_result(&result);

        job.fail(JobFailure::new(JobError::Other, message).with_result_json(result_json));

        return;
    }

    timeline.complete_current(switch_mtls_unset_stage_details(
        &requested_services,
        &report,
    ));

    let result = build_switch_mtls_unset_result(
        "completed",
        result_mode,
        &requested_services,
        &timeline,
        &report,
    );

    let result_json = serialize_switch_mtls_unset_result(&result);

    tracing::info!(
        job_id = %job_id,
        node = %switch.id(),
        rack = %switch.rack_id(),
        result_bytes = result_json.len(),
        "switch mTLS unset job completed"
    );

    job.complete("Completed", result_json);
}

fn dispatch_switch_mtls_unset_job(
    job_tracker: Arc<JobTracker>,
    job: RmsJobHandle,
    switch: Arc<SwitchGb200Nvidia>,
    services: Vec<SwitchMtlsService>,
) -> JobJoinHandle {
    job_tracker.spawn_job(job, move |job| async move {
        run_switch_mtls_unset_job(job, switch, services, SwitchMtlsUnsetMode::InsecureSwitch).await;
    })
}

fn dispatch_switch_mtls_disable_job(
    job_tracker: Arc<JobTracker>,
    job: RmsJobHandle,
    switch: Arc<SwitchGb200Nvidia>,
    services: Vec<SwitchMtlsService>,
) -> JobJoinHandle {
    job_tracker.spawn_job(job, move |job| async move {
        run_switch_mtls_unset_job(job, switch, services, SwitchMtlsUnsetMode::DisableMtls).await;
    })
}

fn prepare_switch_mtls_disable_switch(
    device: &rm::NodeInfo,
    insecure_bmc: bool,
) -> std::result::Result<Arc<SwitchGb200Nvidia>, String> {
    let node_id = device.node_id.clone();
    let rack_id = device.rack_id.clone();
    let node_type_key = device.r#type.unwrap_or(0);
    let node_type = proto_node_type_to_domain(node_type_key);

    tracing::info!(
        node = %node_id,
        rack = %rack_id,
        node_type = node_type_key,
        "processing switch mTLS disable request"
    );

    let Some(node_type) = node_type.filter(|node_type| node_type.kind() == NodeKind::Switch) else {
        let node_type_name = proto_node_type_to_string(node_type_key).unwrap_or("unknown");

        return Err(format!(
            "device {} is not a switch (type={node_type_name})",
            device.node_id
        ));
    };

    let flat = flatten_node_info(device).map_err(|error| error.message)?;

    if flat.creds_for_node_type(node_type).is_none() {
        return Err(format!(
            "Missing host credentials for switch {}",
            device.node_id
        ));
    }

    // Disabling mTLS uses only the host SSH endpoint. A malformed optional BMC
    // endpoint does not prevent the operation.
    let bmc_endpoint = match flat.optional_bmc_endpoint(insecure_bmc) {
        Ok(endpoint) => endpoint,
        Err(error) => {
            tracing::warn!(
                node = %node_id,
                rack = %rack_id,
                error = %error.message,
                "ignoring malformed BMC endpoint for switch mTLS disable"
            );

            None
        }
    };

    let host_endpoint = flat.switch_host_management_endpoint().map_err(|error| {
        format!(
            "Invalid host endpoint for switch {}: {}",
            device.node_id, error.message
        )
    })?;

    let config = NodeConfig {
        id: node_id,
        node_type,
        bmc_endpoint,
        host_endpoint: Some(host_endpoint),
        expected_inventory: None,
        recovery: None,
        runtime_config: None,
    };

    let switch = match node_type {
        DomainNodeType::SwitchGb200Nvidia => SwitchGb200Nvidia::from_config(&config, &rack_id),
        DomainNodeType::SwitchGb300Nvidia => SwitchGb300Nvidia::from_config(&config, &rack_id),
        DomainNodeType::SwitchVrnvl72Nvidia => SwitchVrnvl72Nvidia::from_config(&config, &rack_id)
            .map(SwitchVrnvl72Nvidia::into_shared_switch),
        _ => Err(RmsError::unimplemented("disable switch mTLS", node_type)),
    }
    .map_err(|error| error.message)?;

    #[cfg(test)]
    let switch = switch.with_ssh_exec_for_test(|_| Ok(String::new()));

    Ok(Arc::new(switch))
}

fn reserve_switch_mtls_disable_jobs(
    job_tracker: &JobTracker,
    parent_job_id: &str,
    devices: Vec<rm::NodeInfo>,
    batch: &mut rm::NodeBatchResponse,
    insecure_bmc: bool,
) -> (Vec<QueuedSwitchMtlsDisableJob>, u32) {
    let total_nodes = devices.len() as u32;

    let (reserved, rejected) = job_tracker.create_batch_jobs(
        parent_job_id,
        JobType::SwitchMtlsDisable,
        batch_targets(devices),
        |device| prepare_switch_mtls_disable_switch(device, insecure_bmc),
    );

    batch.node_results.extend(failed_node_results(rejected));

    let skipped = total_nodes - reserved.len() as u32;
    let queued_jobs = reserved
        .into_iter()
        .map(|(device, job, switch)| QueuedSwitchMtlsDisableJob {
            job,
            switch,
            node_id: device.node_id,
        })
        .collect();

    (queued_jobs, skipped)
}

/// Fails the stage at `timeline`'s cursor, marks every stage after it
/// `skipped` (since the job is about to exit early), and seals the job as
/// failed. A thin, job-specific wrapper around `stage_timeline::fail_job`
/// -- see that function's docs for why the shared sequence lives there
/// and not here.
///
/// `outcome` is either `"failed"` or `"cancelled"`; callers should derive
/// it from the triggering error's `ErrorCode` (`"cancelled"` for
/// `ErrorCode::Cancelled`, `"failed"` otherwise) rather than hardcoding
/// `"failed"`, so a cancelled stage -- e.g. an SFTP copy interrupted by
/// `sftp_copy_mtls_file`'s cancellation token -- is recorded and reported
/// as cancelled rather than failed.
///
/// `configured` is threaded in explicitly rather than captured by
/// `build_result_json`, since -- unlike `domain`/`ca_cert_id`/
/// `entity_cert_id`/`remote_dir`, which never change -- it grows across
/// the caller's lifetime as services are bound.
fn fail_stage(
    timeline: &mut StageTimeline,
    job: RmsJobHandle,
    outcome: &str,
    error: &str,
    details: Value,
    configured: &[&str],
    build_result_json: &impl Fn(&str, &[&str], &StageTimeline) -> String,
) {
    fail_job(
        timeline,
        job,
        outcome,
        error,
        details,
        |outcome, timeline| build_result_json(outcome, configured, timeline),
    );
}

struct SwitchCertificateJobProgress<'a> {
    timeline: &'a mut StageTimeline,
    job: &'a RmsJobHandle,
}

impl SwitchCertificateInstallProgress for SwitchCertificateJobProgress<'_> {
    fn start_stage(&mut self, stage: SwitchCertificateInstallStage) -> Result<()> {
        let (actual_name, description) = self.timeline.start_stage()?;
        let expected_name = stage.name();

        if actual_name != expected_name {
            return Err(RmsError::internal(format!(
                "switch certificate stage mismatch: expected {expected_name}, got {actual_name}"
            )));
        }

        self.job.progress(description);

        Ok(())
    }

    fn complete_stage(&mut self, details: Value) {
        self.timeline.complete_current(details);
    }
}

/// Successful output from the shared switch certificate installation.
pub(crate) struct SwitchCertificateInstallResult {
    /// Services that were bound before verification completed.
    pub(crate) configured_services: Vec<&'static str>,

    /// Switch-reported verification details for the configured services.
    pub(crate) service_verification: Value,
}

/// Failure output from the shared switch certificate installation.
#[derive(Debug)]
pub(crate) struct SwitchCertificateInstallFailure {
    /// Error returned by the failed installation phase.
    pub(crate) error: RmsError,

    /// Phase-specific diagnostics suitable for a stage timeline.
    pub(crate) details: Value,

    /// Services successfully bound before the failure occurred.
    pub(crate) configured_services: Vec<&'static str>,
}

impl SwitchCertificateInstallFailure {
    fn new(error: RmsError, details: Value, configured_services: &[&'static str]) -> Self {
        Self {
            error,
            details,
            configured_services: configured_services.to_vec(),
        }
    }
}

/// Panic-safe backstop that removes staged mTLS material from the switch.
///
/// [`install_switch_certificate_material`] calls
/// `cleanup_mtls_remote_dir_best_effort` on every normal success/failure path
/// and [`Self::disarm`]s this guard immediately afterwards, so in the common
/// case its drop is a no-op. Its sole purpose is the one path those explicit,
/// awaited calls cannot cover: an unexpected unwind or early drop after the
/// client private key has been staged but before it is imported.
///
/// Cleanup is async and `Drop` is not, so an armed drop spawns a detached
/// best-effort cleanup task rather than awaiting. That is deliberately a hygiene
/// backstop: the operation future is already being torn down and a leaked
/// scratch directory is recoverable, so a detached best-effort removal is the
/// right trade-off.
struct StagedMaterialCleanupGuard {
    switch: Arc<dyn SwitchCertificateInstallNode>,
    material: SwitchMtlsMaterialPaths,
    armed: bool,
}

impl StagedMaterialCleanupGuard {
    fn armed(
        switch: Arc<dyn SwitchCertificateInstallNode>,
        material: SwitchMtlsMaterialPaths,
    ) -> Self {
        Self {
            switch,
            material,
            armed: true,
        }
    }

    /// Disarm on a normal path once the staged material has already been
    /// removed (or handed off to the switch), making drop a no-op.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StagedMaterialCleanupGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        // Only reachable on an unexpected unwind/early drop -- every normal path
        // disarms. We cannot await here, so hand the cleanup to a detached task.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::error!(
                node = %self.switch.node_id(),
                remote_dir = %self.material.remote_dir(),
                "switch certificate operation dropped with staged mTLS material but no runtime \
                 is available to remove it; manual cleanup may be required"
            );
            return;
        };

        tracing::warn!(
            node = %self.switch.node_id(),
            remote_dir = %self.material.remote_dir(),
            "switch certificate operation dropped with staged mTLS material still present; \
             spawning best-effort cleanup so the client key does not outlive the operation"
        );

        let switch = Arc::clone(&self.switch);
        let material = self.material.clone();
        runtime.spawn(async move {
            switch.cleanup_mtls_remote_dir_best_effort(&material).await;
        });
    }
}

async fn cleanup_staged_certificate_material(
    switch: &Arc<dyn SwitchCertificateInstallNode>,
    material: &SwitchMtlsMaterialPaths,
    cleanup_guard: &mut StagedMaterialCleanupGuard,
) {
    switch.cleanup_mtls_remote_dir_best_effort(material).await;
    cleanup_guard.disarm();
}

async fn transfer_and_import_switch_certificate_material(
    switch: &Arc<dyn SwitchCertificateInstallNode>,
    material: &SwitchMtlsMaterialPaths,
    cancel: &CancellationToken,
    progress: &mut dyn SwitchCertificateInstallProgress,
) -> std::result::Result<(), SwitchCertificateInstallFailure> {
    progress
        .start_stage(SwitchCertificateInstallStage::PrepareRemoteDirectory)
        .map_err(|error| SwitchCertificateInstallFailure::new(error, json!({}), &[]))?;

    if let Err(error) = switch.prepare_mtls_remote_dirs(material).await {
        let details = json!({ "error": &error.message });

        return Err(SwitchCertificateInstallFailure::new(error, details, &[]));
    }

    progress.complete_stage(json!({ "remote_dir": material.remote_dir() }));

    // The guard covers unexpected cancellation or unwind after the remote
    // directory exists. Every ordinary exit below performs awaited cleanup.
    let mut cleanup_guard = StagedMaterialCleanupGuard::armed(Arc::clone(switch), material.clone());

    for spec in mtls_remote_file_specs(material) {
        if let Err(error) = progress.start_stage(SwitchCertificateInstallStage::TransferMaterial(
            spec.remote_name,
        )) {
            cleanup_staged_certificate_material(switch, material, &mut cleanup_guard).await;

            return Err(SwitchCertificateInstallFailure::new(error, json!({}), &[]));
        }

        let copy_result = switch
            .sftp_copy_mtls_file(material, spec.remote_name, cancel.clone())
            .await;

        let copy_outcome = match copy_result {
            Ok(copy_outcome) => copy_outcome,
            Err(error) => {
                cleanup_staged_certificate_material(switch, material, &mut cleanup_guard).await;

                let details = json!({
                    "error": &error.message,
                    "remote_file": spec.remote_name,
                    "local_path": spec.local_path.display().to_string(),
                });

                return Err(SwitchCertificateInstallFailure::new(error, details, &[]));
            }
        };

        progress.complete_stage(json!({
            "remote_file": spec.remote_name,
            "remote_path": copy_outcome.remote_path,
            "bytes": copy_outcome.bytes,
        }));
    }

    if let Err(error) = progress.start_stage(SwitchCertificateInstallStage::ImportMaterial) {
        cleanup_staged_certificate_material(switch, material, &mut cleanup_guard).await;

        return Err(SwitchCertificateInstallFailure::new(error, json!({}), &[]));
    }

    if let Err(error) = switch.import_mtls_material(material).await {
        cleanup_staged_certificate_material(switch, material, &mut cleanup_guard).await;

        let details = json!({ "error": &error.message });

        return Err(SwitchCertificateInstallFailure::new(error, details, &[]));
    }

    cleanup_staged_certificate_material(switch, material, &mut cleanup_guard).await;

    progress.complete_stage(json!({
        "ca_certificate_id": material.ca_cert_id(),
        "entity_certificate_id": material.entity_cert_id(),
    }));

    Ok(())
}

async fn bind_and_verify_switch_certificate_services(
    switch: &Arc<dyn SwitchCertificateInstallNode>,
    material: &SwitchMtlsMaterialPaths,
    nvue_transition: Option<&NvueTlsTransition>,
    services: &[SwitchMtlsService],
    progress: &mut dyn SwitchCertificateInstallProgress,
) -> std::result::Result<SwitchCertificateInstallResult, SwitchCertificateInstallFailure> {
    let mut configured_services = Vec::with_capacity(services.len());

    if certificate_job_discovers_nvue_capabilities(services) {
        progress
            .start_stage(SwitchCertificateInstallStage::DiscoverNvueCapabilities)
            .map_err(|error| {
                SwitchCertificateInstallFailure::new(error, json!({}), &configured_services)
            })?;

        let capabilities = match switch.initialize_nvue_capabilities().await {
            Ok(capabilities) => capabilities,
            Err(error) => {
                let details = json!({ "error": &error.message });

                return Err(SwitchCertificateInstallFailure::new(
                    error,
                    details,
                    &configured_services,
                ));
            }
        };

        progress.complete_stage(json!({
            "gnmi_client_authentication": gnmi_client_authentication_label(capabilities),
        }));
    }

    for service in ordered_bind_services(services) {
        progress
            .start_stage(SwitchCertificateInstallStage::BindService(*service))
            .map_err(|error| {
                SwitchCertificateInstallFailure::new(error, json!({}), &configured_services)
            })?;

        if let Err(error) = switch
            .bind_mtls_service_for_install(material, *service, nvue_transition)
            .await
        {
            let details = json!({
                "error": &error.message,
                "service": service.as_str(),
            });

            return Err(SwitchCertificateInstallFailure::new(
                error,
                details,
                &configured_services,
            ));
        }

        configured_services.push(service.as_str());
        progress.complete_stage(json!({ "service": service.as_str() }));
    }

    progress
        .start_stage(SwitchCertificateInstallStage::VerifyServices)
        .map_err(|error| {
            SwitchCertificateInstallFailure::new(error, json!({}), &configured_services)
        })?;

    let service_verification = switch
        .verify_mtls_services(material, services)
        .await
        .map_err(|error| {
            let details = json!({ "error": &error.message });

            SwitchCertificateInstallFailure::new(error, details, &configured_services)
        })?;

    progress.complete_stage(service_verification.clone());

    Ok(SwitchCertificateInstallResult {
        configured_services,
        service_verification,
    })
}

/// Gives each NVOS certificate store room for this install's import.
///
/// A store with room is left alone, and service bindings are read only when a
/// store is full. A full store loses its oldest RMS-created entries that no
/// switch service uses, just enough for one import. Both stores are planned
/// before anything is deleted, so a store that cannot be reclaimed leaves the
/// other untouched. After deleting, the stores are listed again and the
/// install proceeds only when each has room.
///
/// Every failure here happens before staging, import, or binding, so the
/// switch keeps every entry its services are bound to. Cancellation is checked
/// before the stores are read and before each deletion, so a cancelled job
/// keeps its remaining unused entries.
async fn reclaim_certificate_store_capacity(
    switch: &Arc<dyn SwitchCertificateInstallNode>,
    material: &SwitchMtlsMaterialPaths,
    cancel: &CancellationToken,
) -> std::result::Result<Value, SwitchCertificateInstallFailure> {
    let limit = NVOS_CERTIFICATE_STORE_LIMIT;

    let fail = |error: RmsError, deleted: &[Value]| {
        let details = json!({ "error": &error.message, "deleted": deleted });

        SwitchCertificateInstallFailure::new(error, details, &[])
    };

    let cancelled = |deleted: &[Value]| {
        fail(
            RmsError::cancelled(
                "certificate store reclamation cancelled; nothing was imported or bound",
            ),
            deleted,
        )
    };

    if cancel.is_cancelled() {
        return Err(cancelled(&[]));
    }

    let inventory = switch
        .read_certificate_inventory()
        .await
        .map_err(|error| fail(error, &[]))?;

    let bindings = if inventory.needs_space(limit) {
        switch
            .read_certificate_bindings()
            .await
            .map_err(|error| fail(error, &[]))?
    } else {
        CertificateBindings::default()
    };

    let protected = BTreeSet::from([material.ca_cert_id(), material.entity_cert_id()]);

    let plans = CertificateStore::ALL
        .into_iter()
        .map(|store| {
            plan_store_reclaim(
                store,
                inventory.store(store),
                bindings.store(store),
                &protected,
                limit,
            )
        })
        .collect::<Result<Vec<_>>>()
        .map_err(|error| fail(error, &[]))?;

    let mut deleted = Vec::new();

    for plan in &plans {
        for id in &plan.deletions {
            if cancel.is_cancelled() {
                return Err(cancelled(&deleted));
            }

            if let Err(error) = switch.delete_certificate_entry(plan.store, id).await {
                let error = RmsError::new(
                    error.code,
                    format!(
                        "failed to delete unused NVOS {} entry {id}: {}; nothing was imported \
                         or bound",
                        plan.store, error.message
                    ),
                );

                return Err(fail(error, &deleted));
            }

            deleted.push(json!({ "store": plan.store, "certificate_id": id }));
        }
    }

    if deleted.is_empty() {
        return Ok(json!({ "limit": limit, "stores": plans, "deleted": deleted }));
    }

    let after = switch
        .read_certificate_inventory()
        .await
        .map_err(|error| fail(error, &deleted))?;

    for store in CertificateStore::ALL {
        let entries = after.store(store).entries.len();

        if entries >= limit {
            let error = RmsError::failed_precondition(format!(
                "NVOS {store} store still lists {entries} of {limit} entries after RMS deleted \
                 unused entries; nothing was imported or bound"
            ));

            return Err(fail(error, &deleted));
        }
    }

    tracing::info!(
        node = %switch.node_id(),
        deleted = deleted.len(),
        entity_entries = after.entity.entries.len(),
        ca_entries = after.ca.entries.len(),
        "reclaimed NVOS certificate store capacity"
    );

    Ok(json!({
        "limit": limit,
        "stores": plans,
        "deleted": deleted,
        "entries_after": {
            "entity-certificate": after.entity.entries.len(),
            "ca-certificate": after.ca.entries.len(),
        },
    }))
}

/// Install, bind, and verify switch certificate material without creating or
/// sealing a tracked RMS job.
///
/// The caller owns progress reporting. This operation owns staging cleanup,
/// including the panic-safe backstop that prevents private-key material from
/// being stranded when its future is dropped.
pub(crate) async fn install_switch_certificate_material(
    switch: Arc<dyn SwitchCertificateInstallNode>,
    material: &SwitchMtlsMaterialPaths,
    nvue_transition: Option<&NvueTlsTransition>,
    services: &[SwitchMtlsService],
    cancel: &CancellationToken,
    progress: &mut dyn SwitchCertificateInstallProgress,
) -> std::result::Result<SwitchCertificateInstallResult, SwitchCertificateInstallFailure> {
    progress
        .start_stage(SwitchCertificateInstallStage::ReclaimCertificateCapacity)
        .map_err(|error| SwitchCertificateInstallFailure::new(error, json!({}), &[]))?;

    let reclaimed = reclaim_certificate_store_capacity(&switch, material, cancel).await?;

    progress.complete_stage(reclaimed);

    transfer_and_import_switch_certificate_material(&switch, material, cancel, progress).await?;

    bind_and_verify_switch_certificate_services(
        &switch,
        material,
        nvue_transition,
        services,
        progress,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_switch_certificate_connectivity_tests(
    switch: &SwitchGb200Nvidia,
    services: &[SwitchMtlsService],
    test_hello: bool,
    external_client_tls: Option<&NmxcTlsConfig>,
    switch_host: &str,
    switch_host_port: u16,
    nmx_gateway_id: &str,
    configured_services: &[&'static str],
    progress: &mut dyn SwitchCertificateInstallProgress,
) -> std::result::Result<serde_json::Map<String, Value>, SwitchCertificateInstallFailure> {
    let mut results = serde_json::Map::new();

    if !test_hello {
        return Ok(results);
    }

    if services.contains(&SwitchMtlsService::NvueApi) {
        progress
            .start_stage(SwitchCertificateInstallStage::TestNvueHello)
            .map_err(|error| {
                SwitchCertificateInstallFailure::new(error, json!({}), configured_services)
            })?;

        if let Err(error) = test_nvue_hello_with_retries(switch).await {
            let details = json!({
                "error": &error.message,
                "switch_host": switch_host,
                "switch_host_port": switch_host_port,
            });

            return Err(SwitchCertificateInstallFailure::new(
                error,
                details,
                configured_services,
            ));
        }

        progress.complete_stage(json!({
            "switch_host": switch_host,
            "switch_host_port": switch_host_port,
        }));

        results.insert("nvue_hello_test".into(), json!("success"));
    }

    if let (true, Some(client_tls)) = (
        services.contains(&SwitchMtlsService::ScaleUpFabricManager),
        external_client_tls,
    ) {
        progress
            .start_stage(SwitchCertificateInstallStage::TestNmxHello)
            .map_err(|error| {
                SwitchCertificateInstallFailure::new(error, json!({}), configured_services)
            })?;

        if let Err(error) =
            test_nmx_hello_with_retries(switch, switch_host, client_tls, nmx_gateway_id).await
        {
            let details = json!({
                "error": &error.message,
                "switch_host": switch_host,
                "tls_authority": client_tls.authority.as_deref().unwrap_or("(unknown)"),
            });

            return Err(SwitchCertificateInstallFailure::new(
                error,
                details,
                configured_services,
            ));
        }

        progress.complete_stage(json!({
            "switch_host": switch_host,
            "tls_authority": client_tls.authority.as_deref().unwrap_or("(unknown)"),
        }));

        results.insert("hello_test".into(), json!("success"));
    }

    if let (true, Some(client_tls)) = (
        services.contains(&SwitchMtlsService::ScaleUpFabricTelemetryInterface),
        external_client_tls,
    ) {
        progress
            .start_stage(SwitchCertificateInstallStage::TestGnmiCapabilities)
            .map_err(|error| {
                SwitchCertificateInstallFailure::new(error, json!({}), configured_services)
            })?;

        if let Err(error) =
            test_gnmi_capabilities_with_retries(switch, switch_host, client_tls).await
        {
            let details = json!({
                "error": &error.message,
                "switch_host": switch_host,
                "tls_authority": client_tls.authority.as_deref(),
            });

            return Err(SwitchCertificateInstallFailure::new(
                error,
                details,
                configured_services,
            ));
        }

        progress.complete_stage(json!({
            "switch_host": switch_host,
            "tls_authority": client_tls.authority.as_deref(),
        }));

        results.insert("gnmi_capabilities_test".into(), json!("success"));
    }

    Ok(results)
}

#[allow(clippy::too_many_arguments)]
async fn finish_switch_certificate_job(
    job: RmsJobHandle,
    switch: &SwitchGb200Nvidia,
    material: &SwitchMtlsMaterialPaths,
    services: &[SwitchMtlsService],
    test_hello: bool,
    external_client_tls: Option<&NmxcTlsConfig>,
    switch_host: &str,
    switch_host_port: u16,
    nmx_gateway_id: &str,
    install: SwitchCertificateInstallResult,
    timeline: &mut StageTimeline,
    build_result_json: &impl Fn(&str, &[&str], &StageTimeline) -> String,
) {
    let configured = install.configured_services;
    let mut extra = serde_json::Map::new();

    if let Some(authentication) = recorded_gnmi_client_authentication(&install.service_verification)
    {
        extra.insert("gnmi_client_authentication".into(), authentication);
    }
    extra.insert("service_verification".into(), install.service_verification);

    let connectivity_result = {
        let mut progress = SwitchCertificateJobProgress {
            timeline,
            job: &job,
        };

        run_switch_certificate_connectivity_tests(
            switch,
            services,
            test_hello,
            external_client_tls,
            switch_host,
            switch_host_port,
            nmx_gateway_id,
            &configured,
            &mut progress,
        )
        .await
    };

    let connectivity_results = match connectivity_result {
        Ok(results) => results,
        Err(failure) => {
            let outcome = if failure.error.code == ErrorCode::Cancelled {
                "cancelled"
            } else {
                "failed"
            };

            fail_stage(
                timeline,
                job,
                outcome,
                &failure.error.message,
                failure.details,
                &failure.configured_services,
                build_result_json,
            );

            return;
        }
    };

    extra.extend(connectivity_results);

    let result_json = build_switch_certificate_result_json(
        "completed",
        &material.domain,
        &material.ca_cert_id(),
        &material.entity_cert_id(),
        &material.remote_dir(),
        &configured,
        timeline,
        Value::Object(extra),
    );

    tracing::info!(
        job_id = %job.id(),
        node = %switch.id(),
        rack = %switch.rack_id(),
        domain = %material.domain,
        test_hello,
        configured_services = ?configured,
        result_bytes = result_json.len(),
        "switch certificate configuration job completed"
    );

    job.complete_verified("Completed", result_json);
}

// Per-node orchestrator for ConfigureSwitchCertificate.
// Stages: reclaim_certificate_capacity, prepare_remote_dir, sftp_copy_<file>, import_material,
// discover_nvue_capabilities (gNMI only), bind_<service>, verify_services,
// test_nvue_hello, test_nmx_hello, test_gnmi_capabilities.
#[allow(clippy::too_many_arguments)]
async fn run_switch_certificate_job(
    job: RmsJobHandle,
    switch: Arc<SwitchGb200Nvidia>,
    material: SwitchMtlsMaterialPaths,
    client_tls: Option<StableNmxcTlsConfig>,
    nvue_transition: Option<NvueTlsTransition>,
    services: Vec<SwitchMtlsService>,
    test_hello: bool,
    switch_host: String,
    switch_host_port: u16,
    nmx_gateway_id: String,
) {
    let job_id = job.id().to_string();
    let cancellation_token = job.cancellation_token();
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
    let mut timeline = switch_certificate_job_timeline(
        &job_id,
        &material,
        &services,
        test_hello,
        external_client_tls.is_some(),
        &switch,
    );

    // Builds the `result_json` embedded in every early-exit `fail` seal
    // below (with the full stage timing_summary), parameterized by what
    // varies across those calls: `status` (outcome) and `configured`
    // (services bound before the failure). The final `job.complete` seal
    // has its own call to `build_switch_certificate_result_json` instead,
    // since it needs non-null `extra` fields this always-`Value::Null`
    // closure doesn't support.
    let build_result_json =
        |status: &str, configured: &[&str], timeline: &StageTimeline| -> String {
            build_switch_certificate_result_json(
                status,
                &domain,
                &ca_cert_id,
                &entity_cert_id,
                &remote_dir,
                configured,
                timeline,
                Value::Null,
            )
        };

    if requires_external_client_tls && external_client_tls.is_none() {
        timeline.skip_remaining();
        let error = "post-install NMX/gNMI test requires RMS client TLS material";
        let result_json = build_result_json("failed", &[], &timeline);
        job.fail(JobFailure::new(JobError::ClientError, error).with_result_json(result_json));

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

    let install_switch: Arc<dyn SwitchCertificateInstallNode> = switch.clone();
    let install_result = {
        let mut progress = SwitchCertificateJobProgress {
            timeline: &mut timeline,
            job: &job,
        };

        install_switch_certificate_material(
            install_switch,
            &material,
            nvue_transition.as_ref(),
            &services,
            &cancellation_token,
            &mut progress,
        )
        .await
    };

    let install = match install_result {
        Ok(install) => install,
        Err(failure) => {
            let outcome = if failure.error.code == ErrorCode::Cancelled {
                "cancelled"
            } else {
                "failed"
            };

            fail_stage(
                &mut timeline,
                job,
                outcome,
                &failure.error.message,
                failure.details,
                &failure.configured_services,
                &build_result_json,
            );

            return;
        }
    };

    finish_switch_certificate_job(
        job,
        &switch,
        &material,
        &services,
        test_hello,
        external_client_tls,
        &switch_host,
        switch_host_port,
        &nmx_gateway_id,
        install,
        &mut timeline,
        &build_result_json,
    )
    .await;
}

impl RackManagerServiceImpl {
    /// Ensures the selected primary uses the configured NMX Controller security mode.
    ///
    /// NMX Controller is rebound when its certificate or CA ID differs from
    /// the NVUE API material, or its manager encryption is not `mtls`.
    /// A matching binding uses NMX Hello with bounded connect retries and no
    /// rebind. When the binding cannot be read, a one-shot Hello connect failure
    /// triggers a rebind followed by the retry check.
    pub(crate) async fn enforce_primary_nmx_controller_security(
        &self,
        switch: &dyn SwitchScaleUpManagement,
        switch_host: &str,
        endpoint_server_name: &str,
        request_domain: Option<&str>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        if self.switch_tls_roots.insecure_switch {
            return disable_insecure_nmx_controller_mtls(switch, &cancellation_token).await;
        }

        let switch = switch.as_switch_gb200().ok_or_else(|| {
            RmsError::invalid_argument(format!(
                "switch '{}' does not support NMX Controller mTLS",
                switch.id()
            ))
        })?;

        let client_tls = nmx_client_tls_with_endpoint_authority(
            self.switch_tls_roots
                .resolve_client_tls(request_domain)
                .map_err(RmsError::failed_precondition)?,
            &self
                .switch_tls_roots
                .tls_server_name_for_endpoint(endpoint_server_name, request_domain)
                .map_err(RmsError::failed_precondition)?,
        );

        ensure_primary_nmx_controller_mtls(
            switch.nmx_controller_binding_is_current(),
            async {
                prepare_nmx_controller_for_hello(switch, switch_host).await?;
                test_nmx_hello(switch_host, &client_tls, &self.nmx_gateway_id).await
            },
            async {
                if cancellation_token.is_cancelled() {
                    return Err(RmsError::cancelled(
                        "primary NMX Controller mTLS configuration cancelled before it started",
                    ));
                }

                switch.bind_nmx_controller_to_nvue_api_material().await
            },
            test_nmx_hello_with_retries(switch, switch_host, &client_tls, &self.nmx_gateway_id),
        )
        .await
    }

    /// Runs the `ConfigureSwitchCertificate` checks for one node and builds the
    /// switch its job will drive.
    ///
    /// Rejections are logged here because only this step knows why the node
    /// failed; the caller reports the returned message.
    fn prepare_switch_certificate_node(
        &self,
        device: &rm::NodeInfo,
        mode: &SwitchCertificateMode,
        services: &[SwitchMtlsService],
        request_domain: Option<&str>,
    ) -> std::result::Result<PreparedSwitchCertificateNode, String> {
        let node_id = device.node_id.clone();
        let rack_id = device.rack_id.clone();
        let node_type_key = device.r#type.unwrap_or(0);

        tracing::info!(
            node = %node_id,
            rack = %rack_id,
            domain = mode.domain_for_log(),
            node_type = node_type_key,
            "processing switch certificate configuration request"
        );

        let Some(node_type) = proto_node_type_to_domain(node_type_key)
            .filter(|node_type| node_type.kind() == NodeKind::Switch)
        else {
            tracing::warn!(
                node = %node_id,
                rack = %rack_id,
                node_type = node_type_key,
                "skipping switch certificate configuration for non-switch node"
            );

            return Err(format!(
                "device {} is not a switch (type={})",
                device.node_id,
                proto_node_type_to_string(node_type_key).unwrap_or("unknown")
            ));
        };

        let flat = flatten_node_info(device).map_err(|error| {
            tracing::error!(
                node = %node_id,
                rack = %rack_id,
                error = %error.message,
                "invalid endpoint configuration for switch certificate configuration"
            );

            error.message
        })?;

        if flat.creds_for_node_type(node_type).is_none() {
            tracing::error!(
                node = %node_id,
                rack = %rack_id,
                "missing host credentials for switch certificate configuration"
            );

            return Err(format!(
                "Missing host credentials for switch {}",
                device.node_id
            ));
        }

        let bmc_endpoint = match flat.optional_bmc_endpoint(self.insecure_bmc) {
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
        let host_endpoint = flat.switch_host_management_endpoint().map_err(|e| {
            tracing::error!(
                node = %node_id,
                rack = %rack_id,
                error = %e.message,
                "invalid host endpoint for switch certificate configuration"
            );

            format!(
                "Invalid host endpoint for switch {}: {}",
                device.node_id, e.message
            )
        })?;

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
            self.switch_tls_roots
                .tls_server_name_for_endpoint(endpoint_authority, request_domain)
                .map_err(|message| {
                    tracing::warn!(
                        node = %node_id,
                        rack = %rack_id,
                        endpoint = %endpoint_authority,
                        error = %message,
                        "skipping switch certificate configuration: could not resolve TLS server name"
                    );

                    message
                })?
        };

        let mut client_tls = match mode {
            SwitchCertificateMode::Install(install) => install.client_tls.clone(),
            SwitchCertificateMode::UnsetMtls => None,
        };

        if let Some(client_tls) = client_tls.as_mut() {
            client_tls.set_authority_if_none(&tls_server_name);
        }

        let config = NodeConfig {
            id: node_id.clone(),
            node_type,
            bmc_endpoint,
            host_endpoint: Some(host_endpoint),
            expected_inventory: None,
            recovery: None,
            runtime_config: None,
        };

        let switch = match node_type {
            DomainNodeType::SwitchGb200Nvidia => SwitchGb200Nvidia::from_config(&config, &rack_id),
            DomainNodeType::SwitchGb300Nvidia => SwitchGb300Nvidia::from_config(&config, &rack_id),
            DomainNodeType::SwitchVrnvl72Nvidia => {
                SwitchVrnvl72Nvidia::from_config(&config, &rack_id)
                    .map(SwitchVrnvl72Nvidia::into_shared_switch)
            }
            _ => Err(RmsError::unimplemented(
                "configure switch certificate",
                node_type,
            )),
        };

        let mut registered_nvue_client = None;

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
                        true => {
                            // Keep bootstrap transport changes local to the job. The
                            // registered client is updated after verified mTLS succeeds.
                            registered_nvue_client = Some(Arc::clone(&client));

                            let job_client = client.clone_with_credentials(
                                nvue_client::ClientCredentials::new(username, password),
                            );

                            switch.with_nvue_client(job_client)
                        }
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

        let switch = switch.map_err(|error| {
            tracing::error!(
                node = %node_id,
                rack = %rack_id,
                switch_host = %switch_host,
                switch_host_port,
                error = %error.message,
                "failed to construct switch for certificate configuration"
            );

            error.message
        })?;

        Ok(PreparedSwitchCertificateNode {
            switch: Arc::new(switch),
            client_tls,
            nvue_server_name: tls_server_name,
            registered_nvue_client,
            switch_host,
            switch_host_port,
        })
    }

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
        let parent_rack_id = devices[0].rack_id.clone();
        let Some(parent_id) = self
            .job_tracker
            .create_parent_job(&parent_rack_id, JobType::SwitchCertificate)
        else {
            return Err(tonic::Status::resource_exhausted(
                "failed to create parent switch certificate job",
            ));
        };
        batch.job_id = parent_id.clone();

        let mut jobs = Vec::new();
        let request_domain = r.domain.as_deref();

        let (reserved, rejected) = self.job_tracker.create_batch_jobs(
            &parent_id,
            JobType::SwitchCertificate,
            batch_targets(devices),
            |device| self.prepare_switch_certificate_node(device, &mode, &services, request_domain),
        );

        batch.node_results.extend(failed_node_results(rejected));

        let skipped = total_nodes - reserved.len() as u32;

        for (device, job, prepared) in reserved {
            let PreparedSwitchCertificateNode {
                switch,
                client_tls: node_client_tls,
                nvue_server_name,
                registered_nvue_client,
                switch_host,
                switch_host_port,
            } = prepared;
            let node_id = device.node_id;
            let rack_id = device.rack_id;
            let job_id = job.id().to_string();
            jobs.push(rm::ConfigureSwitchCertificateJobInfo {
                node_id: node_id.clone(),
                job_id: job_id.clone(),
            });

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
                    let material_spawn = install.material.clone();
                    let client_tls_spawn = node_client_tls;
                    let switch_host_spawn = switch_host.clone();
                    let test_hello = r.test_hello;
                    let nmx_gateway_id = self.nmx_gateway_id.clone();
                    let service = self.clone();
                    let nvue_client_tls = install.nvue_client_tls.clone();
                    let nvue_server_name_spawn = nvue_server_name;
                    let request_domain = r.domain.clone();

                    self.job_tracker
                        .spawn_job(job, move |job| async move {
                            // This node was reserved during whole-batch admission.
                            // Perform network work only inside the admitted worker
                            // so rejected nodes are never touched.
                            if let Err(error) = service
                                .ensure_nvue_client_for_certificate(
                                    switch.optional_nvue_client(),
                                    request_domain.as_deref(),
                                    Some(&nvue_server_name_spawn),
                                )
                                .await
                            {
                                tracing::warn!(
                                    job_id = %job.id(),
                                    node = %switch.id(),
                                    rack = %switch.rack_id(),
                                    switch_host = %switch_host_spawn,
                                    switch_host_port,
                                    error = %error.message,
                                    "switch certificate job failed: NVUE client readiness check failed"
                                );
                                job.fail(JobFailure::new(JobError::ClientError, error.message));
                                return;
                            }

                            let nvue_transition = match prepare_nvue_tls_transition(
                                switch.optional_nvue_client(),
                                nvue_client_tls.as_ref(),
                                nvue_server_name_spawn,
                                registered_nvue_client,
                            )
                            .await
                            {
                                Ok(transition) => transition,
                                Err(error) => {
                                    tracing::warn!(
                                        job_id = %job.id(),
                                        node = %switch.id(),
                                        rack = %switch.rack_id(),
                                        switch_host = %switch_host_spawn,
                                        switch_host_port,
                                        error = %error.message,
                                        "switch certificate job failed: failed to prepare NVUE TLS transition"
                                    );
                                    job.fail(JobFailure::new(JobError::ClientError, error.message));
                                    return;
                                }
                            };

                            run_switch_certificate_job(
                                job,
                                switch,
                                material_spawn,
                                client_tls_spawn,
                                nvue_transition,
                                services_spawn,
                                test_hello,
                                switch_host_spawn,
                                switch_host_port,
                                nmx_gateway_id,
                            )
                            .await;
                        })
                        .detach();
                }
                SwitchCertificateMode::UnsetMtls => {
                    dispatch_switch_mtls_unset_job(
                        self.job_tracker.clone(),
                        job,
                        switch,
                        services_spawn,
                    )
                    .detach();
                }
            }
        }

        let jobs_created = jobs.len() as u32;
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
            self.job_tracker
                .mark_failed_message(&parent_id, &batch.message);

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
            parent_rack_id,
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

    /// Starts asynchronous jobs that disable mTLS for selected switch services.
    pub(crate) async fn handle_batch_disable_switch_mtls(
        &self,
        req: tonic::Request<rm::BatchDisableSwitchMtlsRequest>,
    ) -> std::result::Result<tonic::Response<rm::BatchDisableSwitchMtlsResponse>, tonic::Status>
    {
        let request = req.into_inner();

        let services =
            parse_switch_services(&request.services).map_err(tonic::Status::invalid_argument)?;

        let devices = request.nodes.map(|nodes| nodes.nodes).unwrap_or_default();

        let Some(parent_rack_id) = devices.first().map(|device| device.rack_id.as_str()) else {
            return Err(tonic::Status::invalid_argument(
                "nodes is required and must contain at least one device",
            ));
        };

        let total_nodes = devices.len() as u32;

        let mut batch = rm::NodeBatchResponse {
            status: rm::ReturnCode::Failure.into(),
            ..Default::default()
        };

        tracing::info!(
            total_nodes,
            service_count = services.len(),
            "BatchDisableSwitchMtls request received"
        );

        let Some(parent_id) = self
            .job_tracker
            .create_parent_job(parent_rack_id, JobType::SwitchMtlsDisable)
        else {
            batch.message = "parent switch mTLS disable job was not created".into();

            return Ok(tonic::Response::new(rm::BatchDisableSwitchMtlsResponse {
                response: Some(batch),
            }));
        };

        batch.job_id = parent_id.clone();

        let (queued_jobs, skipped) = reserve_switch_mtls_disable_jobs(
            self.job_tracker.as_ref(),
            &parent_id,
            devices,
            &mut batch,
            self.insecure_bmc,
        );

        let jobs_created = queued_jobs.len() as u32;

        if queued_jobs.is_empty() {
            batch.message = "No switch mTLS disable jobs created".into();
            self.job_tracker
                .mark_failed_message(&parent_id, &batch.message);

            batch.stats = Some(rm::NodeOperationStats {
                total_nodes,
                successful_nodes: 0,
                failed_nodes: skipped,
            });

            return Ok(tonic::Response::new(rm::BatchDisableSwitchMtlsResponse {
                response: Some(batch),
            }));
        }

        for queued in queued_jobs {
            batch.node_results.push(rm::NodeOperationResult {
                node_id: queued.node_id,
                status: rm::ReturnCode::Success.into(),
                error_message: String::new(),
            });

            dispatch_switch_mtls_disable_job(
                self.job_tracker.clone(),
                queued.job,
                queued.switch,
                services.clone(),
            )
            .detach();
        }

        batch.status = if skipped == 0 {
            rm::ReturnCode::Success
        } else {
            rm::ReturnCode::Failure
        }
        .into();

        batch.message = format!(
            "Created {jobs_created} switch mTLS disable jobs out of {total_nodes} nodes. \
             Use GetJobStatus with job_id to track progress."
        );

        batch.stats = Some(rm::NodeOperationStats {
            total_nodes,
            successful_nodes: jobs_created,
            failed_nodes: skipped,
        });

        tracing::info!(
            parent_job_id = %batch.job_id,
            jobs_created,
            total_nodes,
            skipped,
            ?services,
            "BatchDisableSwitchMtls jobs dispatched"
        );

        Ok(tonic::Response::new(rm::BatchDisableSwitchMtlsResponse {
            response: Some(batch),
        }))
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
    use std::future::ready;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::api::grpc::server::SwitchTlsRoots;
    use crate::nodes::nvos::GnmiClientCa;
    use crate::nodes::switch_mtls::{
        GNMI_CLIENT_AUTHENTICATION_MTLS, GNMI_CLIENT_AUTHENTICATION_SERVER_CERTIFICATE_ONLY,
    };
    use crate::orchestrator::job_lifecycle::JobState;
    use crate::orchestrator::rack_manager::RackManager;
    use crate::persistence::Backends;

    #[derive(Default)]
    struct RecordingCertificateInstallProgress {
        started: Vec<SwitchCertificateInstallStage>,
        completed: Vec<Value>,
    }

    impl SwitchCertificateInstallProgress for RecordingCertificateInstallProgress {
        fn start_stage(&mut self, stage: SwitchCertificateInstallStage) -> Result<()> {
            self.started.push(stage);
            Ok(())
        }

        fn complete_stage(&mut self, details: Value) {
            self.completed.push(details);
        }
    }

    #[derive(Default)]
    struct FakeCertificateInstallNode {
        operations: Mutex<Vec<String>>,

        /// Remote file whose SFTP copy fails.
        failing_copy: Option<&'static str>,
    }

    impl FakeCertificateInstallNode {
        fn record(&self, operation: impl Into<String>) {
            self.operations.lock().unwrap().push(operation.into());
        }
    }

    #[async_trait]
    impl SwitchCertificateInstallNode for FakeCertificateInstallNode {
        fn node_id(&self) -> &str {
            "test-switch"
        }

        async fn read_certificate_inventory(&self) -> Result<CertificateInventory> {
            self.record("inventory");
            Ok(CertificateInventory::default())
        }

        async fn read_certificate_bindings(&self) -> Result<CertificateBindings> {
            self.record("bindings");
            Ok(CertificateBindings::default())
        }

        async fn delete_certificate_entry(&self, store: CertificateStore, id: &str) -> Result<()> {
            self.record(format!("delete:{store}:{id}"));
            Ok(())
        }

        async fn prepare_mtls_remote_dirs(
            &self,
            _material: &SwitchMtlsMaterialPaths,
        ) -> Result<()> {
            self.record("prepare");
            Ok(())
        }

        async fn sftp_copy_mtls_file(
            &self,
            material: &SwitchMtlsMaterialPaths,
            remote_file_name: &str,
            _cancel: CancellationToken,
        ) -> Result<MtlsSftpCopyOutcome> {
            self.record(format!("copy:{remote_file_name}"));

            if self.failing_copy == Some(remote_file_name) {
                return Err(RmsError::unavailable("SFTP upload failed"));
            }

            Ok(MtlsSftpCopyOutcome {
                remote_path: format!("{}/{remote_file_name}", material.remote_dir()),
                bytes: 1,
            })
        }

        async fn import_mtls_material(&self, _material: &SwitchMtlsMaterialPaths) -> Result<()> {
            self.record("import");
            Ok(())
        }

        async fn initialize_nvue_capabilities(&self) -> Result<GnmiClientCa> {
            self.record("discover");
            Ok(GnmiClientCa::Unsupported)
        }

        async fn cleanup_mtls_remote_dir_best_effort(&self, _material: &SwitchMtlsMaterialPaths) {
            self.record("cleanup");
        }

        async fn bind_mtls_service_for_install(
            &self,
            _material: &SwitchMtlsMaterialPaths,
            service: SwitchMtlsService,
            _nvue_transition: Option<&NvueTlsTransition>,
        ) -> Result<()> {
            self.record(format!("bind:{}", service.as_str()));
            Ok(())
        }

        async fn verify_mtls_services(
            &self,
            _material: &SwitchMtlsMaterialPaths,
            _services: &[SwitchMtlsService],
        ) -> Result<Value> {
            self.record("verify");
            Ok(json!({ "verified": true }))
        }
    }

    fn test_service(switch_tls_roots: SwitchTlsRoots) -> RackManagerServiceImpl {
        RackManagerServiceImpl {
            rack_manager: Arc::new(RackManager::new()),
            job_tracker: Arc::new(JobTracker::new()),
            firmware_download_cancellations: Arc::new(Mutex::new(HashMap::new())),
            backends: Backends::memory(),
            firmware_dir: PathBuf::from("firmware"),
            artifact_require_sha256: false,
            insecure_bmc: true,
            artifact_download_policy:
                crate::api::grpc::artifact_download::ArtifactDownloadPolicy::default(),
            switch_tls_roots,
            sftp_upload_options: crate::transport::ssh::SshUploadOptions::default(),
            nmx_gateway_id: crate::libnmxc::DEFAULT_NMX_C_GATEWAY_ID.to_owned(),
            expected_inventory_catalog: crate::config::ExpectedInventoryCatalog::default(),
            workflow_timings: crate::config::WorkflowTimings::default(),
            recovery_profiles: crate::config::RecoveryProfiles::default(),
            fabric_operations: std::sync::Arc::new(Default::default()),
            fabric_layout_catalog: std::sync::Arc::new(
                crate::config::fabric_layout::FabricLayoutCatalog::default(),
            ),
            hardware_profiles: Arc::new(
                crate::config::hardware_profiles::HardwareProfileCatalog::built_in().unwrap(),
            ),
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
            }),
            ..Default::default()
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

    #[tokio::test]
    async fn switch_certificate_job_stages_discovers_nvue_capabilities_only_for_gnmi() {
        let dir = tempfile::tempdir().expect("temp dir");
        let ca_cert_path = dir.path().join("ca.pem");
        let entity_cert_path = dir.path().join("client.pem");
        let entity_key_path = dir.path().join("client.key");
        std::fs::write(&ca_cert_path, b"ca").expect("write ca.pem");
        std::fs::write(&entity_cert_path, b"cert").expect("write client.pem");
        std::fs::write(&entity_key_path, b"key").expect("write client.key");

        let material = SwitchMtlsMaterialPaths::load_stable(
            "example.com".to_owned(),
            ca_cert_path,
            entity_cert_path,
            entity_key_path,
        )
        .await
        .expect("load_stable");

        let nvue_names: Vec<String> =
            switch_certificate_job_stages(&material, &[SwitchMtlsService::NvueApi], false, false)
                .iter()
                .map(|stage| stage.name().to_owned())
                .collect();
        assert!(
            !nvue_names
                .iter()
                .any(|name| name == "discover_nvue_capabilities"),
            "NVUE-only jobs must not plan capability discovery: {nvue_names:?}"
        );

        let gnmi_names: Vec<String> = switch_certificate_job_stages(
            &material,
            &[SwitchMtlsService::ScaleUpFabricTelemetryInterface],
            false,
            false,
        )
        .iter()
        .map(|stage| stage.name().to_owned())
        .collect();
        let import = gnmi_names
            .iter()
            .position(|name| name == "import_material")
            .expect("import_material");
        let initialize = gnmi_names
            .iter()
            .position(|name| name == "discover_nvue_capabilities")
            .expect("discover_nvue_capabilities");
        let bind = gnmi_names
            .iter()
            .position(|name| name == "bind_scale-up-fabric-telemetry-interface")
            .expect("bind gNMI");
        assert_eq!(initialize, import + 1);
        assert_eq!(bind, initialize + 1);
    }

    #[test]
    fn switch_mtls_unset_job_timeline_attaches_node_and_rack() {
        // Regression test: `run_switch_mtls_unset_job` builds its
        // `StageTimeline` through this helper specifically so the
        // node/rack it runs against are attached -- without this, the
        // "stage starting"/"stage completed"/"stage failed" logs
        // `StageTimeline` emits end up with empty node/rack fields.
        let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:1");
        let timeline = switch_mtls_unset_job_timeline("job-1", &switch);
        assert_eq!(timeline.node(), "test-switch");
        assert_eq!(timeline.rack(), "test-rack");
    }

    #[tokio::test]
    async fn switch_certificate_job_timeline_attaches_node_and_rack() {
        // Regression test: `run_switch_certificate_job` builds its
        // `StageTimeline` through this helper specifically so the
        // node/rack it runs against are attached. See
        // `switch_mtls_unset_job_timeline_attaches_node_and_rack`.
        let dir = tempfile::tempdir().expect("temp dir");
        let ca_cert_path = dir.path().join("ca.pem");
        let entity_cert_path = dir.path().join("client.pem");
        let entity_key_path = dir.path().join("client.key");
        std::fs::write(&ca_cert_path, b"ca").expect("write ca.pem");
        std::fs::write(&entity_cert_path, b"cert").expect("write client.pem");
        std::fs::write(&entity_key_path, b"key").expect("write client.key");

        let material = SwitchMtlsMaterialPaths::load_stable(
            "example.com".to_owned(),
            ca_cert_path,
            entity_cert_path,
            entity_key_path,
        )
        .await
        .expect("load_stable");

        let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:1");
        let timeline = switch_certificate_job_timeline(
            "job-1",
            &material,
            &[SwitchMtlsService::NvueApi],
            false,
            false,
            &switch,
        );

        assert_eq!(timeline.node(), "test-switch");
        assert_eq!(timeline.rack(), "test-rack");
    }

    #[tokio::test]
    async fn run_switch_certificate_job_attaches_result_json_when_client_tls_missing() {
        // Regression test: the "requires_external_client_tls" precondition
        // check fails the job before any stage starts, but must still
        // attach a result_json with the (fully skipped) timeline -- just
        // like every other failure path -- instead of discarding it.
        let dir = tempfile::tempdir().expect("temp dir");
        let ca_cert_path = dir.path().join("ca.pem");
        let entity_cert_path = dir.path().join("client.pem");
        let entity_key_path = dir.path().join("client.key");
        std::fs::write(&ca_cert_path, b"ca").expect("write ca.pem");
        std::fs::write(&entity_cert_path, b"cert").expect("write client.pem");
        std::fs::write(&entity_key_path, b"key").expect("write client.key");

        let material = SwitchMtlsMaterialPaths::load_stable(
            "example.com".to_owned(),
            ca_cert_path,
            entity_cert_path,
            entity_key_path,
        )
        .await
        .expect("load_stable");

        let tracker = Arc::new(JobTracker::new());
        let job = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchCertificate)
            .expect("create switch certificate job");
        let job_id = job.id().to_string();

        let switch = Arc::new(SwitchGb200Nvidia::for_test("http://127.0.0.1"));

        run_switch_certificate_job(
            job,
            switch,
            material,
            None,
            None,
            vec![SwitchMtlsService::ScaleUpFabricManager],
            true,
            "sw-01.example.com".to_owned(),
            443,
            "gateway-1".to_owned(),
        )
        .await;

        let info = tracker.get_job(&job_id).expect("switch certificate job");
        assert_eq!(info.state, JobState::Failed);
        assert!(info.error_message.contains("RMS client TLS material"));

        let result: Value =
            serde_json::from_str(&info.result_json).expect("result_json should parse");
        assert_eq!(result["status"], "failed");
        let stages = result["timing_summary"]["stages"].as_array().unwrap();
        assert!(!stages.is_empty());
        assert!(stages.iter().all(|stage| stage["status"] == "skipped"));
    }

    #[tokio::test]
    async fn run_switch_certificate_job_records_cancelled_outcome_when_cancelled() {
        // Regression test: a stage can return `RmsError::cancelled` (here,
        // store reclamation sees the job's token already cancelled), but
        // `fail_stage` previously hardcoded
        // `timeline.fail_current(false, ...)` and a `"failed"` result
        // status regardless of the triggering error's code, so a
        // cancelled certificate job was misreported as failed. Compare to
        // `switch_image_handlers.rs`'s equivalent
        // `stage_two_push_cancelled_error_records_cancelled_outcome`.
        let dir = tempfile::tempdir().expect("temp dir");
        let ca_cert_path = dir.path().join("ca.pem");
        let entity_cert_path = dir.path().join("client.pem");
        let entity_key_path = dir.path().join("client.key");
        std::fs::write(&ca_cert_path, b"ca").expect("write ca.pem");
        std::fs::write(&entity_cert_path, b"cert").expect("write client.pem");
        std::fs::write(&entity_key_path, b"key").expect("write client.key");

        let material = SwitchMtlsMaterialPaths::load_stable(
            "example.com".to_owned(),
            ca_cert_path,
            entity_cert_path,
            entity_key_path,
        )
        .await
        .expect("load_stable");

        let tracker = Arc::new(JobTracker::new());
        let job = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchCertificate)
            .expect("create switch certificate job");
        let job_id = job.id().to_string();

        // Cancel before the job starts, so reclamation returns
        // `RmsError::cancelled` before touching the stores.
        job.cancellation_token().cancel();

        let switch = Arc::new(
            SwitchGb200Nvidia::for_test("http://127.0.0.1")
                .with_ssh_exec_for_test(|_| Ok(String::new())),
        );

        run_switch_certificate_job(
            job,
            switch,
            material,
            None,
            None,
            vec![SwitchMtlsService::ScaleUpFabricManager],
            false,
            "sw-01.example.com".to_owned(),
            443,
            "gateway-1".to_owned(),
        )
        .await;

        let info = tracker.get_job(&job_id).expect("switch certificate job");
        assert_eq!(info.state, JobState::Failed);
        assert!(info.error_message.contains("cancelled"));

        let result: Value =
            serde_json::from_str(&info.result_json).expect("result_json should parse");
        assert_eq!(
            result["status"], "cancelled",
            "a cancelled job should record a cancelled outcome, not failed"
        );
    }

    #[tokio::test]
    async fn install_cleans_up_staged_material_when_sftp_copy_fails() {
        // Regression test for the "always clean staged mTLS material" invariant.
        // When an SFTP copy fails, the installer must remove the scratch dir
        // before returning, so the client private key never outlives the
        // failed attempt. `cleanup_mtls_remote_dir_removes_the_staged_directory`
        // pins that the cleanup issues the `rm -rf`.
        let (material, _dir) = staged_test_material().await;
        let switch = Arc::new(FakeCertificateInstallNode {
            failing_copy: Some("client.key"),
            ..FakeCertificateInstallNode::default()
        });
        let install_switch: Arc<dyn SwitchCertificateInstallNode> = switch.clone();
        let mut progress = RecordingCertificateInstallProgress::default();

        let outcome = install_switch_certificate_material(
            install_switch,
            &material,
            None,
            &[SwitchMtlsService::ScaleUpFabricManager],
            &CancellationToken::new(),
            &mut progress,
        )
        .await;
        let Err(failure) = outcome else {
            panic!("install with a failing SFTP copy succeeded");
        };

        assert_eq!(failure.error.code, ErrorCode::Unavailable);
        assert_eq!(failure.details["remote_file"], "client.key");
        assert_eq!(
            *switch.operations.lock().unwrap(),
            vec![
                "inventory",
                "prepare",
                "copy:ca.pem",
                "copy:client.pem",
                "copy:client.key",
                "cleanup",
            ]
        );
    }

    /// Write throwaway PEMs and load them as stable mTLS material for the
    /// certificate-job cleanup tests below. Returns the material plus the temp
    /// dir (kept alive by the caller so the files outlive the load).
    async fn staged_test_material() -> (SwitchMtlsMaterialPaths, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("temp dir");
        let ca_cert_path = dir.path().join("ca.pem");
        let entity_cert_path = dir.path().join("client.pem");
        let entity_key_path = dir.path().join("client.key");
        std::fs::write(&ca_cert_path, b"ca").expect("write ca.pem");
        std::fs::write(&entity_cert_path, b"cert").expect("write client.pem");
        std::fs::write(&entity_key_path, b"key").expect("write client.key");

        let material = SwitchMtlsMaterialPaths::load_stable(
            "example.com".to_owned(),
            ca_cert_path,
            entity_cert_path,
            entity_key_path,
        )
        .await
        .expect("load_stable");
        (material, dir)
    }

    fn recording_switch() -> (Arc<SwitchGb200Nvidia>, Arc<Mutex<Vec<String>>>) {
        let commands = Arc::new(Mutex::new(Vec::<String>::new()));
        let observed = Arc::clone(&commands);
        let switch = Arc::new(
            SwitchGb200Nvidia::for_test("http://127.0.0.1").with_ssh_exec_for_test(
                move |command| {
                    observed.lock().unwrap().push(command.to_owned());
                    Ok(String::new())
                },
            ),
        );
        (switch, commands)
    }

    /// B2 regression test: on the import-stage SUCCESS path, once
    /// `import_mtls_material` returns Ok the job must issue the scratch-dir
    /// `rm -rf <remote_dir>` exactly once so the staged client private key never
    /// outlives a successful import. Comment #7's fix covered the sftp-copy
    /// failure branch; this covers the newer import-success cleanup call site
    /// that fix did not reach.
    ///
    /// The import-*failure* branch shares the identical
    /// `cleanup_mtls_remote_dir_best_effort` call but is not drivable end-to-end
    /// with this harness: the `with_ssh_exec_for_test` seam that records the
    /// SSH-side cleanup also short-circuits the NVUE import that would otherwise
    /// fail, so import always succeeds here. This success test and the panic-safe
    /// guard tests below together exercise the shared cleanup call.
    #[tokio::test]
    async fn run_switch_certificate_job_cleans_up_staged_material_after_successful_import() {
        let (material, _dir) = staged_test_material().await;
        let remote_dir = material.remote_dir().as_str().to_owned();

        let tracker = Arc::new(JobTracker::new());
        let job = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchCertificate)
            .expect("create switch certificate job");

        // Under the test stub prepare/sftp/import/bind are skipped and succeed,
        // so the run reaches -- and only reaches -- the import-success cleanup.
        let (switch, commands) = recording_switch();

        run_switch_certificate_job(
            job,
            switch,
            material,
            None,
            None,
            vec![SwitchMtlsService::ScaleUpFabricManager],
            false,
            "sw-01.example.com".to_owned(),
            443,
            "gateway-1".to_owned(),
        )
        .await;

        let issued = commands.lock().expect("command log");
        let cleanups = issued
            .iter()
            .filter(|command| command.contains("rm -rf") && command.contains(&remote_dir))
            .count();
        assert_eq!(
            cleanups, 1,
            "a successful import must clean up the scratch dir exactly once, got: {issued:?}"
        );
    }

    #[tokio::test]
    async fn shared_certificate_install_operation_runs_without_a_tracked_job() {
        let (material, _dir) = staged_test_material().await;
        let switch = Arc::new(FakeCertificateInstallNode::default());
        let install_switch: Arc<dyn SwitchCertificateInstallNode> = switch.clone();
        let mut progress = RecordingCertificateInstallProgress::default();

        let result = install_switch_certificate_material(
            install_switch,
            &material,
            None,
            &[SwitchMtlsService::ScaleUpFabricManager],
            &CancellationToken::new(),
            &mut progress,
        )
        .await
        .expect("job-independent certificate install should succeed");

        assert_eq!(
            progress.started,
            vec![
                SwitchCertificateInstallStage::ReclaimCertificateCapacity,
                SwitchCertificateInstallStage::PrepareRemoteDirectory,
                SwitchCertificateInstallStage::TransferMaterial("ca.pem"),
                SwitchCertificateInstallStage::TransferMaterial("client.pem"),
                SwitchCertificateInstallStage::TransferMaterial("client.key"),
                SwitchCertificateInstallStage::ImportMaterial,
                SwitchCertificateInstallStage::BindService(SwitchMtlsService::ScaleUpFabricManager,),
                SwitchCertificateInstallStage::VerifyServices,
            ]
        );

        assert_eq!(progress.completed.len(), progress.started.len());

        assert_eq!(
            result.configured_services,
            vec![SwitchMtlsService::ScaleUpFabricManager.as_str()]
        );

        assert_eq!(result.service_verification, json!({ "verified": true }));

        assert_eq!(
            *switch.operations.lock().unwrap(),
            vec![
                "inventory",
                "prepare",
                "copy:ca.pem",
                "copy:client.pem",
                "copy:client.key",
                "import",
                "cleanup",
                "bind:scale-up-fabric-manager",
                "verify",
            ]
        );
    }

    #[tokio::test]
    async fn discover_stage_records_unsupported_gnmi_client_authentication() {
        let (material, _dir) = staged_test_material().await;
        let switch = Arc::new(FakeCertificateInstallNode::default());
        let install_switch: Arc<dyn SwitchCertificateInstallNode> = switch.clone();
        let mut progress = RecordingCertificateInstallProgress::default();

        install_switch_certificate_material(
            install_switch,
            &material,
            None,
            &[SwitchMtlsService::ScaleUpFabricTelemetryInterface],
            &CancellationToken::new(),
            &mut progress,
        )
        .await
        .expect("gNMI certificate install should succeed");

        assert!(progress.completed.iter().any(|details| {
            details["gnmi_client_authentication"]
                == GNMI_CLIENT_AUTHENTICATION_SERVER_CERTIFICATE_ONLY
        }));
        assert!(
            switch
                .operations
                .lock()
                .unwrap()
                .iter()
                .any(|operation| operation == "discover")
        );
    }

    /// Installs against NVOS certificate stores that enforce their entry limit
    /// on import.
    mod certificate_store_capacity {
        use std::collections::BTreeMap;

        use chrono::{NaiveDate, NaiveDateTime, TimeDelta};
        use wiremock::matchers::{body_json, body_string_contains, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        use super::*;
        use crate::nodes::switch_mtls::{ReportedUsage, StoreInventory};

        const LIMIT: usize = NVOS_CERTIFICATE_STORE_LIMIT;

        const ENTITY_COLLECTION: &str = "/nvue_v1/system/security/certificate";
        const CA_COLLECTION: &str = "/nvue_v1/system/security/ca-certificate";

        fn imported_at(index: u32) -> NaiveDateTime {
            NaiveDate::from_ymd_opt(2026, 1, 1)
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .expect("valid date")
                + TimeDelta::seconds(i64::from(index))
        }

        /// Material for the `index`th import. Every index reads the same PEM
        /// paths, so the CA material never changes between installs.
        fn material(index: u32) -> SwitchMtlsMaterialPaths {
            SwitchMtlsMaterialPaths::for_test_at(imported_at(index))
        }

        fn ids(material: &SwitchMtlsMaterialPaths) -> (String, String) {
            (material.entity_cert_id(), material.ca_cert_id())
        }

        #[derive(Default)]
        struct Faults {
            store_listing: bool,
            unreadable_binding: Option<SwitchMtlsService>,
            delete: Option<CertificateStore>,

            /// Deletes in this store report success but leave the entry listed.
            kept_after_delete: Option<CertificateStore>,

            /// Cancelled once the first deletion succeeds.
            cancel_after_delete: Option<CancellationToken>,
            entity_import: bool,
            bind: Option<SwitchMtlsService>,
            verify: bool,
        }

        #[derive(Default)]
        struct SwitchState {
            entity: BTreeSet<String>,
            ca: BTreeSet<String>,

            /// Service name to the `(entity, CA)` IDs it is bound to.
            bindings: BTreeMap<&'static str, (String, String)>,

            deleted: Vec<(CertificateStore, String)>,
            peak_entity: usize,
            peak_ca: usize,
            imports: usize,
            faults: Faults,
        }

        impl SwitchState {
            fn store(&self, store: CertificateStore) -> &BTreeSet<String> {
                match store {
                    CertificateStore::Entity => &self.entity,
                    CertificateStore::Ca => &self.ca,
                }
            }

            fn bound_ids(&self, store: CertificateStore) -> BTreeSet<&String> {
                self.bindings
                    .values()
                    .map(|(entity, ca)| match store {
                        CertificateStore::Entity => entity,
                        CertificateStore::Ca => ca,
                    })
                    .collect()
            }

            fn assert_every_binding_is_listed(&self) {
                for store in CertificateStore::ALL {
                    for id in self.bound_ids(store) {
                        assert!(
                            self.store(store).contains(id),
                            "bound {store} entry {id} is gone"
                        );
                    }
                }
            }
        }

        /// A switch whose stores refuse an import once they hold [`LIMIT`]
        /// entries, independently of RMS's accounting, and whose services keep
        /// the entries they were last bound to. Deleting a bound or non-RMS
        /// entry panics the test.
        #[derive(Default)]
        struct StoreEnforcingSwitch {
            state: Mutex<SwitchState>,
        }

        impl StoreEnforcingSwitch {
            fn seeded(entity: Vec<String>, ca: Vec<String>) -> Arc<Self> {
                let switch = Self::default();
                {
                    let mut state = switch.state();
                    state.entity.extend(entity);
                    state.ca.extend(ca);
                }
                Arc::new(switch)
            }

            fn state(&self) -> std::sync::MutexGuard<'_, SwitchState> {
                self.state.lock().unwrap()
            }
        }

        #[async_trait]
        impl SwitchCertificateInstallNode for StoreEnforcingSwitch {
            fn node_id(&self) -> &str {
                "sw-01"
            }

            async fn read_certificate_inventory(&self) -> Result<CertificateInventory> {
                let state = self.state();

                if state.faults.store_listing {
                    return Err(RmsError::unavailable("NVUE returned 503"));
                }

                // NVOS reports the NVUE API and gNMI server as consumers of
                // their entity certificate; cluster apps are not reported.
                let reported = |id: &String| {
                    let services: Vec<String> = [
                        (SwitchMtlsService::NvueApi, "nvue-rest-api"),
                        (
                            SwitchMtlsService::ScaleUpFabricTelemetryInterface,
                            "gnmi-server",
                        ),
                    ]
                    .into_iter()
                    .filter(|(service, _)| {
                        state
                            .bindings
                            .get(service.as_str())
                            .is_some_and(|(entity, _)| entity == id)
                    })
                    .map(|(_, consumer)| consumer.to_owned())
                    .collect();

                    if services.is_empty() {
                        ReportedUsage::None
                    } else {
                        ReportedUsage::Services(services)
                    }
                };

                Ok(CertificateInventory {
                    entity: StoreInventory {
                        entries: state
                            .entity
                            .iter()
                            .map(|id| (id.clone(), reported(id)))
                            .collect(),
                    },
                    ca: StoreInventory {
                        entries: state
                            .ca
                            .iter()
                            .map(|id| (id.clone(), ReportedUsage::None))
                            .collect(),
                    },
                })
            }

            async fn read_certificate_bindings(&self) -> Result<CertificateBindings> {
                let state = self.state();
                let mut bindings = CertificateBindings::default();

                for service in SwitchMtlsService::ALL {
                    for store in CertificateStore::ALL {
                        let read = if state.faults.unreadable_binding == Some(service) {
                            Err("GET timed out".to_owned())
                        } else {
                            Ok(state
                                .bindings
                                .get(service.as_str())
                                .map(|(entity, ca)| match store {
                                    CertificateStore::Entity => entity.clone(),
                                    CertificateStore::Ca => ca.clone(),
                                })
                                .into_iter()
                                .collect())
                        };

                        bindings.record(store, service, read);
                    }
                }

                Ok(bindings)
            }

            async fn delete_certificate_entry(
                &self,
                store: CertificateStore,
                id: &str,
            ) -> Result<()> {
                let mut state = self.state();

                assert!(id.starts_with("rms-"), "deleted non-RMS {store} entry {id}");
                assert!(
                    !state.bound_ids(store).contains(&id.to_owned()),
                    "deleted bound {store} entry {id}"
                );

                if state.faults.delete == Some(store) {
                    return Err(RmsError::internal("NVUE returned 500"));
                }

                if state.faults.kept_after_delete == Some(store) {
                    return Ok(());
                }

                let removed = match store {
                    CertificateStore::Entity => state.entity.remove(id),
                    CertificateStore::Ca => state.ca.remove(id),
                };
                assert!(removed, "deleted missing {store} entry {id}");

                state.deleted.push((store, id.to_owned()));

                if let Some(cancel) = &state.faults.cancel_after_delete {
                    cancel.cancel();
                }

                Ok(())
            }

            async fn prepare_mtls_remote_dirs(
                &self,
                _material: &SwitchMtlsMaterialPaths,
            ) -> Result<()> {
                Ok(())
            }

            async fn sftp_copy_mtls_file(
                &self,
                material: &SwitchMtlsMaterialPaths,
                remote_file_name: &str,
                _cancel: CancellationToken,
            ) -> Result<MtlsSftpCopyOutcome> {
                Ok(MtlsSftpCopyOutcome {
                    remote_path: format!("{}/{remote_file_name}", material.remote_dir()),
                    bytes: 1,
                })
            }

            async fn import_mtls_material(&self, material: &SwitchMtlsMaterialPaths) -> Result<()> {
                let mut state = self.state();
                state.imports += 1;

                if state.ca.len() >= LIMIT {
                    return Err(RmsError::failed_precondition(
                        "CA certificate store is full",
                    ));
                }
                state.ca.insert(material.ca_cert_id());
                state.peak_ca = state.peak_ca.max(state.ca.len());

                if state.faults.entity_import {
                    return Err(RmsError::internal("entity certificate import failed"));
                }

                if state.entity.len() >= LIMIT {
                    return Err(RmsError::failed_precondition(
                        "entity certificate store is full",
                    ));
                }
                state.entity.insert(material.entity_cert_id());
                state.peak_entity = state.peak_entity.max(state.entity.len());

                Ok(())
            }

            async fn initialize_nvue_capabilities(&self) -> Result<GnmiClientCa> {
                Ok(GnmiClientCa::Unsupported)
            }

            async fn cleanup_mtls_remote_dir_best_effort(
                &self,
                _material: &SwitchMtlsMaterialPaths,
            ) {
            }

            async fn bind_mtls_service_for_install(
                &self,
                material: &SwitchMtlsMaterialPaths,
                service: SwitchMtlsService,
                _nvue_transition: Option<&NvueTlsTransition>,
            ) -> Result<()> {
                let mut state = self.state();

                if state.faults.bind == Some(service) {
                    return Err(RmsError::internal(format!(
                        "bind {} failed",
                        service.as_str()
                    )));
                }

                let (entity, ca) = ids(material);
                assert!(
                    state.entity.contains(&entity) && state.ca.contains(&ca),
                    "bound material that was never imported"
                );

                state.bindings.insert(service.as_str(), (entity, ca));
                Ok(())
            }

            async fn verify_mtls_services(
                &self,
                material: &SwitchMtlsMaterialPaths,
                services: &[SwitchMtlsService],
            ) -> Result<Value> {
                let state = self.state();

                if state.faults.verify {
                    return Err(RmsError::failed_precondition("verification failed"));
                }

                for service in services {
                    assert_eq!(state.bindings.get(service.as_str()), Some(&ids(material)));
                }

                Ok(json!({ "verified": true }))
            }
        }

        type InstallOutcome =
            std::result::Result<SwitchCertificateInstallResult, SwitchCertificateInstallFailure>;

        async fn install(
            switch: &Arc<StoreEnforcingSwitch>,
            material: &SwitchMtlsMaterialPaths,
            services: &[SwitchMtlsService],
            cancel: &CancellationToken,
        ) -> (InstallOutcome, RecordingCertificateInstallProgress) {
            let node: Arc<dyn SwitchCertificateInstallNode> = switch.clone();
            let mut progress = RecordingCertificateInstallProgress::default();

            let outcome = install_switch_certificate_material(
                node,
                material,
                None,
                services,
                cancel,
                &mut progress,
            )
            .await;

            (outcome, progress)
        }

        async fn install_ok(
            switch: &Arc<StoreEnforcingSwitch>,
            material: &SwitchMtlsMaterialPaths,
            services: &[SwitchMtlsService],
        ) {
            if let (Err(failure), _) =
                install(switch, material, services, &CancellationToken::new()).await
            {
                panic!(
                    "install of {} failed: {}",
                    material.entity_cert_id(),
                    failure.error.message
                );
            }
        }

        async fn install_err(
            switch: &Arc<StoreEnforcingSwitch>,
            material: &SwitchMtlsMaterialPaths,
            services: &[SwitchMtlsService],
        ) -> (
            SwitchCertificateInstallFailure,
            RecordingCertificateInstallProgress,
        ) {
            match install(switch, material, services, &CancellationToken::new()).await {
                (Err(failure), progress) => (failure, progress),
                (Ok(_), _) => panic!("install of {} succeeded", material.entity_cert_id()),
            }
        }

        /// `count` unused RMS entries per store, imported at indices
        /// `0..count`.
        fn rms_seed(count: u32) -> (Vec<String>, Vec<String>) {
            (0..count).map(|index| ids(&material(index))).unzip()
        }

        fn site_seed(prefix: &str, count: usize) -> Vec<String> {
            (0..count)
                .map(|index| format!("site-{prefix}-{index}"))
                .collect()
        }

        #[tokio::test]
        async fn more_than_25_installs_stay_within_both_store_limits() {
            let switch = Arc::new(StoreEnforcingSwitch::default());

            for index in 0..40 {
                install_ok(&switch, &material(index), &SwitchMtlsService::ALL).await;
            }

            let state = switch.state();
            assert_eq!(state.imports, 40);
            assert_eq!((state.peak_entity, state.peak_ca), (LIMIT, LIMIT));

            for service in SwitchMtlsService::ALL {
                assert_eq!(state.bindings[service.as_str()], ids(&material(39)));
            }

            assert_eq!(
                state.deleted[..2],
                [
                    (CertificateStore::Entity, material(0).entity_cert_id()),
                    (CertificateStore::Ca, material(0).ca_cert_id()),
                ]
            );
        }

        #[tokio::test]
        async fn services_omitted_from_later_installs_keep_their_oldest_entries() {
            let switch = Arc::new(StoreEnforcingSwitch::default());
            let first = material(0);

            install_ok(&switch, &first, &SwitchMtlsService::ALL).await;

            for index in 1..=40 {
                install_ok(&switch, &material(index), &[SwitchMtlsService::NvueApi]).await;
            }

            let state = switch.state();
            assert!(state.peak_entity <= LIMIT && state.peak_ca <= LIMIT);

            for service in [
                SwitchMtlsService::ScaleUpFabricTelemetry,
                SwitchMtlsService::ScaleUpFabricManager,
                SwitchMtlsService::ScaleUpFabricTelemetryInterface,
            ] {
                assert_eq!(state.bindings[service.as_str()], ids(&first));
            }
            assert_eq!(
                state.bindings[SwitchMtlsService::NvueApi.as_str()],
                ids(&material(40))
            );
            state.assert_every_binding_is_listed();
        }

        #[tokio::test]
        async fn installs_reclaim_when_either_or_both_stores_start_full() {
            // (non-RMS entity, RMS entity, non-RMS CA, RMS CA, deletions per store)
            let cases = [
                (3, 22, 0, 9, (1, 0)),
                (0, 5, 4, 21, (0, 1)),
                (2, 23, 6, 19, (1, 1)),
            ];

            for (site_entity, rms_entity, site_ca, rms_ca, expected) in cases {
                let (entity, _) = rms_seed(rms_entity);
                let (_, ca) = rms_seed(rms_ca);
                let switch = StoreEnforcingSwitch::seeded(
                    [site_seed("cert", site_entity), entity].concat(),
                    [site_seed("ca", site_ca), ca].concat(),
                );

                // The oldest RMS entries stay bound to a service the request omits.
                switch.state().bindings.insert(
                    SwitchMtlsService::ScaleUpFabricTelemetry.as_str(),
                    ids(&material(0)),
                );

                install_ok(&switch, &material(1000), &[SwitchMtlsService::NvueApi]).await;

                let state = switch.state();
                let deleted = |store| {
                    state
                        .deleted
                        .iter()
                        .filter(|(deleted, _)| *deleted == store)
                        .map(|(_, id)| id.clone())
                        .collect::<Vec<_>>()
                };

                let expected_ids = |store, count: usize| {
                    (0..count)
                        .map(|_| match store {
                            CertificateStore::Entity => material(1).entity_cert_id(),
                            CertificateStore::Ca => material(1).ca_cert_id(),
                        })
                        .collect::<Vec<_>>()
                };

                assert_eq!(
                    deleted(CertificateStore::Entity),
                    expected_ids(CertificateStore::Entity, expected.0),
                    "case {site_entity}/{rms_entity}/{site_ca}/{rms_ca}"
                );
                assert_eq!(
                    deleted(CertificateStore::Ca),
                    expected_ids(CertificateStore::Ca, expected.1),
                    "case {site_entity}/{rms_entity}/{site_ca}/{rms_ca}"
                );
                assert!(state.peak_entity <= LIMIT && state.peak_ca <= LIMIT);
                assert!(
                    site_seed("cert", site_entity)
                        .iter()
                        .all(|id| state.entity.contains(id))
                );
                assert!(
                    site_seed("ca", site_ca)
                        .iter()
                        .all(|id| state.ca.contains(id))
                );
                state.assert_every_binding_is_listed();
            }
        }

        #[tokio::test]
        async fn a_store_without_reclaimable_entries_fails_before_any_change() {
            let (entity, _) = rms_seed(25);
            let (_, bound_ca) = ids(&material(0));
            let switch = StoreEnforcingSwitch::seeded(
                entity,
                [site_seed("ca", 24), vec![bound_ca]].concat(),
            );
            switch.state().bindings.insert(
                SwitchMtlsService::ScaleUpFabricManager.as_str(),
                ids(&material(0)),
            );

            let (failure, progress) =
                install_err(&switch, &material(1000), &[SwitchMtlsService::NvueApi]).await;

            assert_eq!(failure.error.code, ErrorCode::FailedPrecondition);
            assert!(
                failure
                    .error
                    .message
                    .contains("ca-certificate store is full (25 of 25 entries, 1 created by RMS)"),
                "{}",
                failure.error.message
            );
            assert_eq!(
                progress.started,
                [SwitchCertificateInstallStage::ReclaimCertificateCapacity]
            );

            // The entity store could have been reclaimed, but nothing is
            // deleted unless both stores can make room.
            let state = switch.state();
            assert!(state.deleted.is_empty());
            assert_eq!(state.imports, 0);
            assert_eq!(state.bindings.len(), 1);
        }

        #[tokio::test]
        async fn an_unconfirmed_binding_blocks_reclaim() {
            let (entity, ca) = rms_seed(25);
            let switch = StoreEnforcingSwitch::seeded(entity, ca);
            switch.state().faults.unreadable_binding =
                Some(SwitchMtlsService::ScaleUpFabricTelemetry);

            let (failure, _) =
                install_err(&switch, &material(1000), &[SwitchMtlsService::NvueApi]).await;

            assert_eq!(failure.error.code, ErrorCode::FailedPrecondition);
            assert!(
                failure
                    .error
                    .message
                    .contains("scale-up-fabric-telemetry: GET timed out"),
                "{}",
                failure.error.message
            );

            let state = switch.state();
            assert!(state.deleted.is_empty());
            assert_eq!(state.imports, 0);
        }

        #[tokio::test]
        async fn a_failed_store_listing_fails_before_any_change() {
            let switch = Arc::new(StoreEnforcingSwitch::default());
            switch.state().faults.store_listing = true;

            let (failure, progress) =
                install_err(&switch, &material(0), &[SwitchMtlsService::NvueApi]).await;

            assert_eq!(failure.error.code, ErrorCode::Unavailable);
            assert_eq!(
                progress.started,
                [SwitchCertificateInstallStage::ReclaimCertificateCapacity]
            );
            assert_eq!(switch.state().imports, 0);
        }

        #[tokio::test]
        async fn a_failed_deletion_fails_before_import_and_reports_what_was_deleted() {
            let (entity, ca) = rms_seed(25);
            let switch = StoreEnforcingSwitch::seeded(entity, ca);
            switch.state().faults.delete = Some(CertificateStore::Ca);

            let (failure, _) =
                install_err(&switch, &material(1000), &[SwitchMtlsService::NvueApi]).await;

            assert_eq!(failure.error.code, ErrorCode::Internal);
            assert!(
                failure.error.message.contains(&format!(
                    "failed to delete unused NVOS ca-certificate entry {}",
                    material(0).ca_cert_id()
                )),
                "{}",
                failure.error.message
            );
            assert!(
                failure
                    .error
                    .message
                    .contains("nothing was imported or bound")
            );
            assert_eq!(
                failure.details["deleted"],
                json!([{
                    "store": "entity-certificate",
                    "certificate_id": material(0).entity_cert_id(),
                }])
            );
            assert_eq!(switch.state().imports, 0);
        }

        #[tokio::test]
        async fn a_store_still_full_after_deletion_fails_before_import() {
            let (entity, ca) = rms_seed(25);
            let switch = StoreEnforcingSwitch::seeded(entity, ca);
            switch.state().faults.kept_after_delete = Some(CertificateStore::Ca);

            let (failure, progress) =
                install_err(&switch, &material(1000), &[SwitchMtlsService::NvueApi]).await;

            assert_eq!(failure.error.code, ErrorCode::FailedPrecondition);
            assert!(
                failure.error.message.contains(
                    "NVOS ca-certificate store still lists 25 of 25 entries after RMS deleted \
                     unused entries; nothing was imported or bound"
                ),
                "{}",
                failure.error.message
            );
            assert_eq!(
                progress.started,
                [SwitchCertificateInstallStage::ReclaimCertificateCapacity]
            );
            assert_eq!(
                failure.details["deleted"],
                json!([
                    {
                        "store": "entity-certificate",
                        "certificate_id": material(0).entity_cert_id(),
                    },
                    {
                        "store": "ca-certificate",
                        "certificate_id": material(0).ca_cert_id(),
                    },
                ])
            );

            let state = switch.state();
            assert_eq!(state.imports, 0);
            assert!(state.bindings.is_empty());
        }

        #[tokio::test]
        async fn a_cancelled_install_deletes_nothing() {
            let (entity, ca) = rms_seed(25);
            let switch = StoreEnforcingSwitch::seeded(entity, ca);
            let cancel = CancellationToken::new();
            cancel.cancel();

            let (outcome, progress) = install(
                &switch,
                &material(1000),
                &[SwitchMtlsService::NvueApi],
                &cancel,
            )
            .await;
            let Err(failure) = outcome else {
                panic!("cancelled install succeeded");
            };

            assert_eq!(failure.error.code, ErrorCode::Cancelled);
            assert_eq!(
                progress.started,
                [SwitchCertificateInstallStage::ReclaimCertificateCapacity]
            );
            assert_eq!(failure.details["deleted"], json!([]));

            let state = switch.state();
            assert!(state.deleted.is_empty());
            assert_eq!(state.imports, 0);
        }

        #[tokio::test]
        async fn cancellation_between_deletions_stops_and_reports_what_was_deleted() {
            let (entity, ca) = rms_seed(25);
            let switch = StoreEnforcingSwitch::seeded(entity, ca);
            let cancel = CancellationToken::new();
            switch.state().faults.cancel_after_delete = Some(cancel.clone());

            let (outcome, _) = install(
                &switch,
                &material(1000),
                &[SwitchMtlsService::NvueApi],
                &cancel,
            )
            .await;
            let Err(failure) = outcome else {
                panic!("cancelled install succeeded");
            };

            assert_eq!(failure.error.code, ErrorCode::Cancelled);
            assert_eq!(
                failure.details["deleted"],
                json!([{
                    "store": "entity-certificate",
                    "certificate_id": material(0).entity_cert_id(),
                }])
            );

            let state = switch.state();
            assert_eq!(
                state.deleted,
                [(CertificateStore::Entity, material(0).entity_cert_id())]
            );
            assert!(state.ca.contains(&material(0).ca_cert_id()));
            assert_eq!(state.imports, 0);
        }

        #[tokio::test]
        async fn later_installs_reclaim_imports_left_by_failed_jobs() {
            let switch = Arc::new(StoreEnforcingSwitch::default());
            let first = material(0);
            install_ok(&switch, &first, &SwitchMtlsService::ALL).await;

            // The entity import fails after the CA import, leaving an unbound CA.
            let orphaned = material(1);
            switch.state().faults.entity_import = true;
            install_err(&switch, &orphaned, &[SwitchMtlsService::NvueApi]).await;
            switch.state().faults.entity_import = false;
            assert!(switch.state().ca.contains(&orphaned.ca_cert_id()));

            // NMX Controller binds before the NVUE API, so a failed NVUE bind
            // leaves the two services on different entries.
            let partial = material(2);
            switch.state().faults.bind = Some(SwitchMtlsService::NvueApi);
            install_err(
                &switch,
                &partial,
                &[
                    SwitchMtlsService::NvueApi,
                    SwitchMtlsService::ScaleUpFabricManager,
                ],
            )
            .await;
            switch.state().faults.bind = None;

            // Verification fails after the bind took effect.
            let unverified = material(3);
            switch.state().faults.verify = true;
            install_err(
                &switch,
                &unverified,
                &[SwitchMtlsService::ScaleUpFabricTelemetry],
            )
            .await;
            switch.state().faults.verify = false;

            switch.state().assert_every_binding_is_listed();

            for index in 4..60 {
                install_ok(
                    &switch,
                    &material(index),
                    &[SwitchMtlsService::ScaleUpFabricTelemetryInterface],
                )
                .await;
            }

            let state = switch.state();
            assert!(
                state
                    .deleted
                    .contains(&(CertificateStore::Ca, orphaned.ca_cert_id()))
            );
            assert_eq!(state.bindings["nvue-api"], ids(&first));
            assert_eq!(state.bindings["scale-up-fabric-manager"], ids(&partial));
            assert_eq!(
                state.bindings["scale-up-fabric-telemetry"],
                ids(&unverified)
            );
            assert!(state.peak_entity <= LIMIT && state.peak_ca <= LIMIT);
            state.assert_every_binding_is_listed();
        }

        fn store_listing(
            store: CertificateStore,
            active: &SwitchMtlsMaterialPaths,
            superseded: &SwitchMtlsMaterialPaths,
            unused: &[SwitchMtlsMaterialPaths],
        ) -> Value {
            let id = |material: &SwitchMtlsMaterialPaths| match store {
                CertificateStore::Entity => material.entity_cert_id(),
                CertificateStore::Ca => material.ca_cert_id(),
            };

            let mut listing = serde_json::Map::new();
            let active_entry = match store {
                CertificateStore::Entity => json!({ "installed": ["nvue-rest-api"] }),
                CertificateStore::Ca => json!({}),
            };
            listing.insert(id(active), active_entry);
            listing.insert(id(superseded), json!({}));
            for material in unused {
                listing.insert(id(material), json!({}));
            }

            Value::Object(listing)
        }

        /// After a reclaim, ConfigureScaleUpFabricManager V2 still binds NMX
        /// Controller to the certificate and CA the NVUE API is using, and the
        /// reclaim kept the superseded entries NMX Controller was bound to.
        #[tokio::test]
        async fn nmx_controller_binds_to_the_active_nvue_material_after_reclaim() {
            let server = MockServer::start().await;

            let active = material(100);
            let superseded = material(50);
            let unused: Vec<_> = (0..23).map(material).collect();

            for (store, collection) in [
                (CertificateStore::Entity, ENTITY_COLLECTION),
                (CertificateStore::Ca, CA_COLLECTION),
            ] {
                Mock::given(method("GET"))
                    .and(path(collection))
                    .respond_with(ResponseTemplate::new(200).set_body_json(store_listing(
                        store,
                        &active,
                        &superseded,
                        &unused,
                    )))
                    .up_to_n_times(1)
                    .mount(&server)
                    .await;

                Mock::given(method("GET"))
                    .and(path(collection))
                    .respond_with(ResponseTemplate::new(200).set_body_json(store_listing(
                        store,
                        &active,
                        &superseded,
                        &unused[1..],
                    )))
                    .mount(&server)
                    .await;
            }

            let reads = [
                (
                    "/nvue_v1/system/api",
                    json!({ "certificate": active.entity_cert_id() }),
                ),
                (
                    "/nvue_v1/system/api/mtls",
                    json!({ "ca-certificate": active.ca_cert_id() }),
                ),
                (
                    "/nvue_v1/cluster",
                    json!({ "state": "enabled", "nmxc-conn": { "state": "up" } }),
                ),
                (
                    "/nvue_v1/cluster/apps/nmx-controller",
                    json!({
                        "status": "ok",
                        "additional-info": "CONTROL_PLANE_STATE_CONFIGURED",
                        "manager": { "state": "enabled" }
                    }),
                ),
                (
                    "/nvue_v1/cluster/apps/nmx-controller/manager",
                    json!({ "state": "enabled" }),
                ),
                (
                    "/nvue_v1/cluster/apps/nmx-controller/manager/encryption",
                    json!({ "encryption": "mtls" }),
                ),
            ];
            for (endpoint, body) in reads {
                Mock::given(method("GET"))
                    .and(path(endpoint))
                    .respond_with(ResponseTemplate::new(200).set_body_json(body))
                    .mount(&server)
                    .await;
            }

            for endpoint in [
                "/nvue_v1/system/gnmi-server",
                "/nvue_v1/system/gnmi-server/mtls",
                "/nvue_v1/cluster/apps/nmx-telemetry/manager/certificate",
                "/nvue_v1/cluster/apps/nmx-telemetry/manager/ca-certificate",
            ] {
                Mock::given(method("GET"))
                    .and(path(endpoint))
                    .respond_with(ResponseTemplate::new(404))
                    .mount(&server)
                    .await;
            }

            // NMX Controller reads back its superseded binding during the
            // reclaim and the active material once it has been rebound.
            for (field, superseded_id, active_id) in [
                (
                    "certificate",
                    superseded.entity_cert_id(),
                    active.entity_cert_id(),
                ),
                (
                    "ca-certificate",
                    superseded.ca_cert_id(),
                    active.ca_cert_id(),
                ),
            ] {
                let endpoint = format!("/nvue_v1/cluster/apps/nmx-controller/manager/{field}");

                Mock::given(method("GET"))
                    .and(path(endpoint.clone()))
                    .respond_with(
                        ResponseTemplate::new(200).set_body_json(json!({ field: superseded_id })),
                    )
                    .up_to_n_times(1)
                    .mount(&server)
                    .await;

                Mock::given(method("GET"))
                    .and(path(endpoint.clone()))
                    .respond_with(
                        ResponseTemplate::new(200).set_body_json(json!({ field: active_id })),
                    )
                    .mount(&server)
                    .await;

                Mock::given(method("POST"))
                    .and(path(endpoint))
                    .and(body_string_contains(active_id.as_str()))
                    .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
                    .expect(1)
                    .mount(&server)
                    .await;
            }

            Mock::given(method("POST"))
                .and(path(
                    "/nvue_v1/cluster/apps/nmx-controller/manager/encryption",
                ))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
                .mount(&server)
                .await;

            for (collection_entry, action_id) in [
                (
                    format!("{ENTITY_COLLECTION}/{}", unused[0].entity_cert_id()),
                    "delete-entity",
                ),
                (
                    format!("{CA_COLLECTION}/{}", unused[0].ca_cert_id()),
                    "delete-ca",
                ),
            ] {
                Mock::given(method("POST"))
                    .and(path(collection_entry))
                    .and(body_json(json!({ "@delete": { "state": "start" } })))
                    .respond_with(ResponseTemplate::new(201).set_body_json(action_id))
                    .expect(1)
                    .mount(&server)
                    .await;

                Mock::given(method("GET"))
                    .and(path(format!("/nvue_v1/action/{action_id}")))
                    .respond_with(
                        ResponseTemplate::new(200)
                            .set_body_json(json!({ "state": "action_success" })),
                    )
                    .expect(1)
                    .mount(&server)
                    .await;
            }

            Mock::given(method("POST"))
                .and(body_string_contains("@delete"))
                .respond_with(ResponseTemplate::new(500))
                .expect(0)
                .named("@delete of any other entry")
                .mount(&server)
                .await;

            let switch = Arc::new(SwitchGb200Nvidia::for_test(&server.uri()));
            let node: Arc<dyn SwitchCertificateInstallNode> = switch.clone();

            let reclaimed = reclaim_certificate_store_capacity(
                &node,
                &material(200),
                &CancellationToken::new(),
            )
            .await
            .expect("reclaim should succeed");

            assert_eq!(
                reclaimed["deleted"],
                json!([
                    {
                        "store": "entity-certificate",
                        "certificate_id": unused[0].entity_cert_id(),
                    },
                    {
                        "store": "ca-certificate",
                        "certificate_id": unused[0].ca_cert_id(),
                    },
                ])
            );

            switch
                .bind_nmx_controller_to_nvue_api_material()
                .await
                .expect("NMX Controller should bind to the active NVUE material");
        }
    }

    /// B1 regression test: an armed guard dropped without an explicit cleanup --
    /// the panic / early-drop case the straight-line `fail_stage` cleanups cannot
    /// cover -- still removes the staged material so the client private key is
    /// never stranded on the switch.
    #[tokio::test]
    async fn staged_material_cleanup_guard_removes_material_on_armed_drop() {
        let (material, _dir) = staged_test_material().await;
        let remote_dir = material.remote_dir().as_str().to_owned();
        let (switch, commands) = recording_switch();

        drop(StagedMaterialCleanupGuard::armed(switch.clone(), material));

        // Drop spawns a detached cleanup task; give it a chance to run.
        for _ in 0..100 {
            if commands
                .lock()
                .unwrap()
                .iter()
                .any(|c| c.contains("rm -rf") && c.contains(&remote_dir))
            {
                break;
            }
            tokio::task::yield_now().await;
        }

        let issued = commands.lock().expect("command log");
        assert!(
            issued
                .iter()
                .any(|c| c.contains("rm -rf") && c.contains(&remote_dir)),
            "an armed guard drop must clean up staged material, got: {issued:?}"
        );
    }

    /// B1: a disarmed guard (the normal path, where an explicit awaited cleanup
    /// already ran) must not issue a second, redundant cleanup on drop.
    #[tokio::test]
    async fn staged_material_cleanup_guard_is_noop_after_disarm() {
        let (material, _dir) = staged_test_material().await;
        let (switch, commands) = recording_switch();

        {
            let mut guard = StagedMaterialCleanupGuard::armed(switch.clone(), material);
            guard.disarm();
        }

        for _ in 0..50 {
            tokio::task::yield_now().await;
        }

        assert!(
            commands.lock().expect("command log").is_empty(),
            "a disarmed guard must not issue any cleanup"
        );
    }

    #[tokio::test]
    async fn unreadable_binding_with_failed_hello_configures_nmx_manager_then_succeeds() {
        let configured = std::cell::Cell::new(false);
        let verified = std::cell::Cell::new(false);

        ensure_primary_nmx_controller_mtls(
            ready(Err(RmsError::internal("NVUE unavailable for test"))),
            ready(Err(RmsError::internal("NMX Hello connect failed for test"))),
            async {
                configured.set(true);
                Ok(())
            },
            async {
                verified.set(true);
                Ok(())
            },
        )
        .await
        .unwrap();

        assert!(configured.get());
        assert!(verified.get());
    }

    #[tokio::test]
    async fn current_nmx_binding_with_exhausted_hello_retries_reports_error_without_rebinding() {
        let configured = std::cell::Cell::new(false);
        let checked_once = std::cell::Cell::new(false);

        let error = ensure_primary_nmx_controller_mtls(
            ready(Ok(true)),
            async {
                checked_once.set(true);
                Ok(())
            },
            async {
                configured.set(true);
                Ok(())
            },
            ready(Err(RmsError::internal(
                "NMX Hello connect failed after retries",
            ))),
        )
        .await
        .unwrap_err();

        assert!(
            error
                .message
                .contains("NMX Hello connect failed after retries")
        );

        assert!(!checked_once.get());
        assert!(!configured.get());
    }

    #[tokio::test]
    async fn stale_nmx_binding_is_reconfigured_although_hello_still_succeeds() {
        let checked = std::cell::Cell::new(false);
        let configured = std::cell::Cell::new(false);
        let verified = std::cell::Cell::new(false);

        ensure_primary_nmx_controller_mtls(
            ready(Ok(false)),
            async {
                checked.set(true);
                Ok(())
            },
            async {
                configured.set(true);
                Ok(())
            },
            async {
                verified.set(true);
                Ok(())
            },
        )
        .await
        .unwrap();

        assert!(
            !checked.get(),
            "a stale binding must not wait for Hello to fail"
        );
        assert!(configured.get());
        assert!(verified.get());
    }

    #[tokio::test]
    async fn current_nmx_binding_with_successful_hello_is_left_alone() {
        let configured = std::cell::Cell::new(false);

        ensure_primary_nmx_controller_mtls(
            ready(Ok(true)),
            ready(Ok(())),
            async {
                configured.set(true);
                Ok(())
            },
            ready(Ok(())),
        )
        .await
        .unwrap();

        assert!(!configured.get());
    }

    #[tokio::test]
    async fn unreadable_nmx_binding_falls_back_to_hello_check() {
        let configured = std::cell::Cell::new(false);

        ensure_primary_nmx_controller_mtls(
            ready(Err(RmsError::internal("NVUE unavailable for test"))),
            ready(Ok(())),
            async {
                configured.set(true);
                Ok(())
            },
            ready(Ok(())),
        )
        .await
        .unwrap();

        assert!(!configured.get());
    }

    #[tokio::test]
    async fn insecure_primary_nmx_security_unsets_manager_when_cluster_is_unready() {
        let commands = Arc::new(Mutex::new(Vec::new()));
        let observed_commands = Arc::clone(&commands);

        let switch = SwitchGb200Nvidia::for_test("http://127.0.0.1:1").with_ssh_exec_for_test(
            move |command| {
                observed_commands.lock().unwrap().push(command.to_owned());

                Ok(String::new())
            },
        );

        let service = test_service(SwitchTlsRoots {
            insecure_switch: true,
            ..SwitchTlsRoots::default()
        });

        tokio::time::timeout(
            Duration::from_secs(1),
            service.enforce_primary_nmx_controller_security(
                &switch,
                "192.0.2.10",
                "switch.example.com",
                None,
                CancellationToken::new(),
            ),
        )
        .await
        .expect("unready primary cleanup should not wait indefinitely")
        .unwrap();

        assert_eq!(
            *commands.lock().unwrap(),
            vec![
                "nv action restore cluster apps nmx-controller manager encryption",
                "nv config apply --assume-yes",
                "nv config save",
            ]
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
    fn recorded_gnmi_client_authentication_keeps_a_named_outcome() {
        assert_eq!(
            recorded_gnmi_client_authentication(&json!({
                "gnmi_client_authentication": GNMI_CLIENT_AUTHENTICATION_SERVER_CERTIFICATE_ONLY
            })),
            Some(json!(GNMI_CLIENT_AUTHENTICATION_SERVER_CERTIFICATE_ONLY))
        );
        assert_eq!(
            recorded_gnmi_client_authentication(&json!({
                "gnmi_client_authentication": GNMI_CLIENT_AUTHENTICATION_MTLS
            })),
            Some(json!(GNMI_CLIENT_AUTHENTICATION_MTLS))
        );
        assert_eq!(
            recorded_gnmi_client_authentication(&json!({
                "gnmi_client_authentication": Value::Null
            })),
            None
        );
        assert_eq!(recorded_gnmi_client_authentication(&json!({})), None);
    }

    #[test]
    fn build_switch_certificate_result_json_surfaces_server_certificate_only() {
        let timeline = StageTimeline::new();
        let json = build_switch_certificate_result_json(
            "completed",
            "fabric-a",
            "rms-fabric-a-ca",
            "rms-fabric-a-cert",
            "/home/admin/certs/run",
            &["scale-up-fabric-telemetry-interface"],
            &timeline,
            json!({
                "gnmi_client_authentication": GNMI_CLIENT_AUTHENTICATION_SERVER_CERTIFICATE_ONLY
            }),
        );
        let parsed: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["status"], "completed");
        assert_eq!(parsed["services"][0], "scale-up-fabric-telemetry-interface");
        assert_eq!(
            parsed["gnmi_client_authentication"],
            GNMI_CLIENT_AUTHENTICATION_SERVER_CERTIFICATE_ONLY
        );
    }

    #[test]
    fn build_switch_mtls_unset_result_reports_services_not_commands() {
        let timeline = StageTimeline::new();
        let report = MtlsUnsetReport::all_cleared(&[SwitchMtlsService::NvueApi]);

        let result = build_switch_mtls_unset_result(
            "completed",
            SwitchMtlsUnsetMode::InsecureSwitch,
            &["nvue-api"],
            &timeline,
            &report,
        );

        let parsed = serde_json::to_value(result).unwrap();

        assert_eq!(parsed["operation"], "unset_mtls");
        assert_eq!(parsed["mode"], "insecure-switch");
        assert_eq!(parsed["services"], json!(["nvue-api"]));

        assert_eq!(
            parsed["service_results"],
            json!([{ "service": "nvue-api", "stage": "unset_binding", "status": "cleared" }])
        );
        assert_eq!(
            parsed["persist"],
            json!([
                { "stage": "config_apply", "status": "cleared" },
                { "stage": "config_save", "status": "cleared" },
            ])
        );

        // `nv` command text is an NVOS implementation detail and must not
        // appear in a client-visible result, in either mode.
        assert!(parsed.get("commands").is_none());
        assert!(!parsed.to_string().contains("nv unset"));

        // `failure` is omitted rather than null on the success path.
        assert!(parsed.get("failure").is_none());
    }

    /// A persist failure is not attributable to any one service, so `service`
    /// is absent. The stage details and the result JSON must agree on that --
    /// a client reading one surface and then the other must not see the key
    /// appear as `null` in one and vanish in the other.
    #[test]
    fn switch_mtls_unset_stage_details_match_the_result_on_a_persist_failure() {
        let timeline = StageTimeline::new();
        let report = MtlsUnsetReport {
            services: vec![MtlsServiceOutcome {
                service: SwitchMtlsService::NvueApi,
                status: MtlsUnsetStatus::Cleared,
            }],
            persist: vec![MtlsPersistOutcome {
                stage: MtlsUnsetStage::ConfigApply,
                status: MtlsUnsetStatus::Failed,
            }],
            failure: Some(MtlsUnsetFailureSite {
                service: None,
                stage: MtlsUnsetStage::ConfigApply,
                message: "exit status 1".to_owned(),
            }),
            cancelled: false,
        };

        let result = serde_json::to_value(build_switch_mtls_unset_result(
            "failed",
            SwitchMtlsUnsetMode::DisableMtls,
            &["nvue-api"],
            &timeline,
            &report,
        ))
        .unwrap();
        let details = switch_mtls_unset_stage_details(&["nvue-api"], &report);

        assert_eq!(details["failure"], result["failure"]);
        assert!(details["failure"].get("service").is_none());
        assert!(result["failure"].get("service").is_none());

        // The rest of the shared shape must match too, since the two are now
        // rendered from the same typed values.
        for key in ["services", "service_results", "persist"] {
            assert_eq!(details[key], result[key], "stage details diverged on {key}");
        }
    }

    /// An attributed failure carries the service, in both surfaces.
    #[test]
    fn switch_mtls_unset_stage_details_match_the_result_on_a_service_failure() {
        let timeline = StageTimeline::new();
        let report = MtlsUnsetReport {
            services: vec![MtlsServiceOutcome {
                service: SwitchMtlsService::NvueApi,
                status: MtlsUnsetStatus::Failed,
            }],
            persist: vec![MtlsPersistOutcome {
                stage: MtlsUnsetStage::ConfigApply,
                status: MtlsUnsetStatus::NotAttempted,
            }],
            failure: Some(MtlsUnsetFailureSite {
                service: Some(SwitchMtlsService::NvueApi),
                stage: MtlsUnsetStage::UnsetBinding,
                message: "exit status 1".to_owned(),
            }),
            cancelled: false,
        };

        let result = serde_json::to_value(build_switch_mtls_unset_result(
            "failed",
            SwitchMtlsUnsetMode::DisableMtls,
            &["nvue-api"],
            &timeline,
            &report,
        ))
        .unwrap();
        let details = switch_mtls_unset_stage_details(&["nvue-api"], &report);

        assert_eq!(details["failure"], result["failure"]);
        assert_eq!(details["failure"]["service"], "nvue-api");
    }

    #[test]
    fn build_switch_mtls_unset_result_marks_explicit_disable_operation() {
        let timeline = StageTimeline::new();
        let report = MtlsUnsetReport::not_attempted(&[SwitchMtlsService::NvueApi]);

        let result = build_switch_mtls_unset_result(
            "failed",
            SwitchMtlsUnsetMode::DisableMtls,
            &["nvue-api"],
            &timeline,
            &report,
        );

        let parsed = serde_json::to_value(result).unwrap();

        assert_eq!(parsed["operation"], "unset_mtls");
        assert_eq!(parsed["mode"], "disable-mtls");
        assert!(parsed.get("commands").is_none());
        assert_eq!(parsed["service_results"][0]["status"], "not_attempted");
    }

    #[tokio::test]
    async fn run_switch_mtls_unset_job_completes_with_fake_ssh() {
        let tracker = Arc::new(JobTracker::new());
        let job = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchCertificate)
            .expect("create switch certificate job");
        let job_id = job.id().to_string();

        let switch = Arc::new(
            SwitchGb200Nvidia::for_test("http://127.0.0.1")
                .with_ssh_exec_for_test(|_| Ok(String::new())),
        );

        run_switch_mtls_unset_job(
            job,
            switch,
            vec![SwitchMtlsService::NvueApi],
            SwitchMtlsUnsetMode::InsecureSwitch,
        )
        .await;

        let info = tracker.get_job(&job_id).unwrap();
        let result: Value = serde_json::from_str(&info.result_json).unwrap();

        assert_eq!(info.state, JobState::Completed);
        assert_eq!(result["operation"], "unset_mtls");
        assert_eq!(result["mode"], "insecure-switch");

        assert_eq!(result["service_results"][0]["service"], "nvue-api");
        assert_eq!(result["service_results"][0]["status"], "cleared");
        assert!(result.get("commands").is_none());
        assert!(result.get("failure").is_none());

        // The stage details mirror the result, so a client reading either one
        // gets the same account of what happened.
        let stages = result["timing_summary"]["stages"].as_array().unwrap();
        assert_eq!(
            stages[0]["details"]["service_results"][0]["status"],
            "cleared"
        );
    }

    /// Graceful shutdown can cancel this job after `run_flow` has already
    /// issued commands. The result must then describe the work that reached
    /// the switch, not write the whole run off as unattempted.
    #[tokio::test]
    async fn run_switch_mtls_unset_job_reports_work_done_before_cancellation() {
        let tracker = Arc::new(JobTracker::new());
        let job = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchCertificate)
            .expect("create switch certificate job");
        let job_id = job.id().to_string();

        // Cancel once the first command has landed, the way a shutdown
        // partway through the flow would.
        let trigger = job.cancellation_token();
        let switch = Arc::new(
            SwitchGb200Nvidia::for_test("http://127.0.0.1").with_ssh_exec_for_test(move |_| {
                trigger.cancel();

                Ok(String::new())
            }),
        );

        run_switch_mtls_unset_job(
            job,
            switch,
            vec![
                SwitchMtlsService::ScaleUpFabricManager,
                SwitchMtlsService::NvueApi,
            ],
            SwitchMtlsUnsetMode::DisableMtls,
        )
        .await;

        let info = tracker.get_job(&job_id).expect("switch mTLS unset job");
        let result: Value = serde_json::from_str(&info.result_json).unwrap();

        assert_eq!(info.state, JobState::Failed);
        assert_eq!(info.error_code, JobError::Other);
        assert_eq!(result["status"], "cancelled");
        assert!(info.error_message.contains("cancelled"));

        assert_eq!(
            result["service_results"],
            json!([
                {
                    "service": "scale-up-fabric-manager",
                    "stage": "unset_binding",
                    "status": "cleared",
                },
                {
                    "service": "nvue-api",
                    "stage": "unset_binding",
                    "status": "not_attempted",
                },
            ])
        );
        assert_eq!(
            result["persist"],
            json!([
                { "stage": "config_apply", "status": "not_attempted" },
                { "stage": "config_save", "status": "not_attempted" },
            ])
        );

        // Cancelling is not failing, so there is no attributed failure site.
        assert!(result.get("failure").is_none());

        let stages = result["timing_summary"]["stages"].as_array().unwrap();
        assert_eq!(
            stages[0]["details"]["service_results"][0]["status"],
            "cleared"
        );
    }

    /// A run that clears one service and then fails must say so: the cleared
    /// service, the failing one, and the untouched remainder all need to be
    /// distinguishable from each other.
    #[tokio::test]
    async fn run_switch_mtls_unset_job_reports_a_partial_failure() {
        let tracker = Arc::new(JobTracker::new());
        let job = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchCertificate)
            .expect("create switch certificate job");
        let job_id = job.id().to_string();

        let switch = Arc::new(
            SwitchGb200Nvidia::for_test("http://127.0.0.1").with_ssh_exec_for_test(|command| {
                if command == "nv action restore cluster apps nmx-controller manager encryption" {
                    return Err(RmsError::internal("permission denied"));
                }

                Ok(String::new())
            }),
        );

        run_switch_mtls_unset_job(
            job,
            switch,
            vec![
                SwitchMtlsService::NvueApi,
                SwitchMtlsService::ScaleUpFabricManager,
                SwitchMtlsService::ScaleUpFabricTelemetry,
            ],
            SwitchMtlsUnsetMode::DisableMtls,
        )
        .await;

        let info = tracker.get_job(&job_id).expect("switch mTLS unset job");
        let result: Value = serde_json::from_str(&info.result_json).unwrap();

        assert_eq!(info.state, JobState::Failed);
        assert_eq!(result["status"], "failed");

        assert_eq!(
            result["service_results"],
            json!([
                { "service": "nvue-api", "stage": "unset_binding", "status": "cleared" },
                {
                    "service": "scale-up-fabric-manager",
                    "stage": "unset_binding",
                    "status": "failed",
                },
                {
                    "service": "scale-up-fabric-telemetry",
                    "stage": "unset_binding",
                    "status": "not_attempted",
                },
            ])
        );
        assert_eq!(
            result["persist"],
            json!([
                { "stage": "config_apply", "status": "not_attempted" },
                { "stage": "config_save", "status": "not_attempted" },
            ])
        );
        assert_eq!(
            result["failure"],
            json!({
                "service": "scale-up-fabric-manager",
                "stage": "unset_binding",
                "message": "permission denied",
            })
        );

        // The client-visible error names the service and stage, never the
        // command that failed.
        assert!(info.error_message.contains("scale-up-fabric-manager"));
        assert!(info.error_message.contains("unset_binding"));
        assert!(!info.error_message.contains("nv action"));
        assert!(!info.result_json.contains("nv action"));
    }

    /// A real SSH failure names the command it ran: `SshError::CommandFailed`
    /// renders `SSH command {command:?} on {host} failed with ...`. That text
    /// becomes the step's failure message and would otherwise ride the report
    /// out to the client, so the transport strips it. The switch's own
    /// diagnostics have to survive that strip, or the payload is useless.
    #[tokio::test]
    async fn run_switch_mtls_unset_job_strips_command_text_from_a_transport_error() {
        let tracker = Arc::new(JobTracker::new());
        let job = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchCertificate)
            .expect("create switch certificate job");
        let job_id = job.id().to_string();

        let switch = Arc::new(
            SwitchGb200Nvidia::for_test("http://127.0.0.1").with_ssh_exec_for_test(|command| {
                if command == "nv action restore cluster apps nmx-controller manager encryption" {
                    // Mirrors the real `SshError::CommandFailed` rendering.
                    return Err(RmsError::internal(format!(
                        "SSH command {command:?} on 127.0.0.1 failed with exit status 1; \
                         stderr: Error: permission denied"
                    )));
                }

                Ok(String::new())
            }),
        );

        run_switch_mtls_unset_job(
            job,
            switch,
            vec![
                SwitchMtlsService::NvueApi,
                SwitchMtlsService::ScaleUpFabricManager,
            ],
            SwitchMtlsUnsetMode::DisableMtls,
        )
        .await;

        let info = tracker.get_job(&job_id).expect("switch mTLS unset job");

        for surface in [&info.error_message, &info.result_json] {
            assert!(
                !surface.contains("nv action"),
                "command text leaked to a client surface: {surface}"
            );
            assert!(
                surface.contains("permission denied"),
                "the switch's own diagnostics were lost: {surface}"
            );
        }

        // Attribution still works, and the placeholder marks what was removed.
        assert!(info.error_message.contains("scale-up-fabric-manager"));
        assert!(info.error_message.contains("<command>"));
    }

    #[tokio::test]
    async fn run_switch_mtls_unset_job_records_cancellation_in_timeline() {
        let tracker = Arc::new(JobTracker::new());
        let job = tracker
            .create_job("rack-01", "sw-01", JobType::SwitchCertificate)
            .expect("create switch certificate job");
        let job_id = job.id().to_string();

        // Cancel before the run starts, so the flow stops at its first
        // checkpoint and no command reaches the switch.
        job.cancellation_token().cancel();

        let switch = Arc::new(
            SwitchGb200Nvidia::for_test("http://127.0.0.1")
                .with_ssh_exec_for_test(|_| Ok(String::new())),
        );

        run_switch_mtls_unset_job(
            job,
            switch,
            vec![SwitchMtlsService::NvueApi],
            SwitchMtlsUnsetMode::InsecureSwitch,
        )
        .await;

        let info = tracker.get_job(&job_id).expect("switch mTLS unset job");
        assert_eq!(info.state, JobState::Failed);
        assert_eq!(info.error_code, JobError::Other);
        assert!(info.error_message.contains("cancelled"));

        // Cancellation must record the stage failure, skip any remaining
        // stages, and attach the serialized timeline -- just like the
        // other failure paths -- rather than leaving clients with an
        // empty result_json.
        let result: Value =
            serde_json::from_str(&info.result_json).expect("result_json should parse");
        assert_eq!(result["status"], "cancelled");
        let stages = result["timing_summary"]["stages"].as_array().unwrap();
        assert_eq!(stages[0]["status"], "failed");
        assert!(stages[0]["message"].as_str().unwrap().contains("cancelled"));

        // Nothing ran, so the report must say every requested service was
        // never attempted -- not omit them, which would read as "unknown".
        assert_eq!(
            result["service_results"],
            json!([{
                "service": "nvue-api",
                "stage": "unset_binding",
                "status": "not_attempted",
            }])
        );
        assert_eq!(
            result["persist"],
            json!([
                { "stage": "config_apply", "status": "not_attempted" },
                { "stage": "config_save", "status": "not_attempted" },
            ])
        );
        assert!(result.get("failure").is_none());
    }

    #[tokio::test]
    async fn dispatch_switch_mtls_disable_job_stops_before_ssh_when_cancelled() {
        let tracker = Arc::new(JobTracker::new());

        let job = tracker
            .create_job_if_node_idle("rack-01", "sw-01", JobType::SwitchMtlsDisable)
            .expect("create switch mTLS disable job");

        let job_id = job.id().to_string();
        let cancellation_token = job.cancellation_token();
        let commands = Arc::new(Mutex::new(Vec::new()));
        let observed_commands = Arc::clone(&commands);

        let switch = Arc::new(
            SwitchGb200Nvidia::for_test("http://127.0.0.1").with_ssh_exec_for_test(
                move |command| {
                    observed_commands.lock().unwrap().push(command.to_owned());

                    Ok(String::new())
                },
            ),
        );

        cancellation_token.cancel();

        let handle = dispatch_switch_mtls_disable_job(
            Arc::clone(&tracker),
            job,
            switch,
            vec![SwitchMtlsService::NvueApi],
        );

        handle.wait().await.expect("supervisor join");

        let info = tracker.get_job(&job_id).expect("mTLS disable job");

        assert_eq!(info.state, JobState::Failed);
        assert!(info.error_message.contains("cancelled"));
        assert!(commands.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn dispatch_switch_mtls_disable_job_marks_abandoned_after_worker_panic() {
        let tracker = Arc::new(JobTracker::new());

        let job = tracker
            .create_job_if_node_idle("rack-01", "sw-01", JobType::SwitchMtlsDisable)
            .expect("create switch mTLS disable job");

        let job_id = job.id().to_string();

        let switch = Arc::new(
            SwitchGb200Nvidia::for_test("http://127.0.0.1")
                .with_ssh_exec_for_test(|_| panic!("simulated SSH panic")),
        );

        let handle = dispatch_switch_mtls_disable_job(
            Arc::clone(&tracker),
            job,
            switch,
            vec![SwitchMtlsService::NvueApi],
        );

        handle.wait().await.expect("supervisor join");

        let info = tracker.get_job(&job_id).expect("mTLS disable job");

        assert_eq!(info.state, JobState::Failed);
        assert_eq!(
            info.error_message,
            "job abandoned before recording a terminal state"
        );
    }

    /// The insecure-switch dispatch runs the same job as the mTLS disable
    /// dispatch; the reported mode is all that distinguishes them.
    #[tokio::test]
    async fn dispatch_switch_mtls_unset_job_runs_the_job_in_insecure_switch_mode() {
        let tracker = Arc::new(JobTracker::new());

        let job = tracker
            .create_job_if_node_idle("rack-01", "sw-01", JobType::SwitchCertificate)
            .expect("create switch certificate job");

        let job_id = job.id().to_string();

        let switch = Arc::new(
            SwitchGb200Nvidia::for_test("http://127.0.0.1")
                .with_ssh_exec_for_test(|_| Ok(String::new())),
        );

        let handle = dispatch_switch_mtls_unset_job(
            Arc::clone(&tracker),
            job,
            switch,
            vec![SwitchMtlsService::NvueApi],
        );

        handle.wait().await.expect("supervisor join");

        let info = tracker.get_job(&job_id).expect("switch mTLS unset job");
        let result: Value = serde_json::from_str(&info.result_json).unwrap();

        assert_eq!(info.state, JobState::Completed);
        assert_eq!(result["mode"], "insecure-switch");
        assert_eq!(result["service_results"][0]["status"], "cleared");
    }

    #[tokio::test]
    async fn configure_switch_certificate_insecure_mode_accepts_all_switch_types() {
        for (node_type, dns_domain) in [
            (rm::NodeType::SwitchGb200Nvidia, None),
            (rm::NodeType::SwitchGb300Nvidia, None),
            (rm::NodeType::SwitchVrnvl72Nvidia, None),
            (rm::NodeType::SwitchVrnvl72Nvidia, Some("bad host")),
        ] {
            let service = test_service(SwitchTlsRoots {
                default_domain: Some("../bad".into()),
                dns_domain: dns_domain.map(str::to_owned),
                insecure_switch: true,
                ..SwitchTlsRoots::default()
            });
            let mut node = switch_node_info();
            node.r#type = Some(node_type as i32);

            let response = service
                .handle_configure_switch_certificate(tonic::Request::new(
                    rm::ConfigureSwitchCertificateRequest {
                        domain: None,
                        services: vec![rm::SwitchService::NvueApi as i32],
                        nodes: Some(rm::NodeSet { nodes: vec![node] }),
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

            // The job is dispatched detached and talks to a switch that is
            // not there, so this asserts admission only. What the run
            // reports is covered against a fake SSH executor by the
            // `run_switch_mtls_unset_job` tests above.
            assert!(
                service
                    .job_tracker
                    .get_job(&response.jobs[0].job_id)
                    .is_some()
            );
        }
    }

    #[tokio::test]
    async fn configure_switch_certificate_admits_whole_batch_before_workers_start() {
        let service = test_service(SwitchTlsRoots {
            insecure_switch: true,
            ..SwitchTlsRoots::default()
        });
        let first = switch_node_info();
        let mut second = switch_node_info();
        second.node_id = "sw-02".into();
        second
            .host_endpoint
            .as_mut()
            .and_then(|endpoint| endpoint.interface.as_mut())
            .expect("host interface")
            .ip_address = "192.0.2.11".into();

        let response = service
            .handle_configure_switch_certificate(tonic::Request::new(
                rm::ConfigureSwitchCertificateRequest {
                    domain: None,
                    services: vec![rm::SwitchService::NvueApi as i32],
                    nodes: Some(rm::NodeSet {
                        nodes: vec![first, second],
                    }),
                    test_hello: false,
                },
            ))
            .await
            .unwrap()
            .into_inner();

        let batch = response.response.expect("batch response");
        let parent = service
            .job_tracker
            .get_job(&batch.job_id)
            .expect("parent switch certificate job");

        assert_eq!(batch.status, rm::ReturnCode::Success as i32);
        assert_eq!(batch.node_results.len(), 0);
        assert_eq!(response.jobs.len(), 2);
        assert_eq!(parent.child_job_ids.len(), 2);
        assert!(
            parent
                .child_job_ids
                .iter()
                .all(|job_id| service.job_tracker.get_job(job_id).is_some())
        );
    }

    #[tokio::test]
    async fn batch_disable_switch_mtls_works_without_insecure_switch_configuration() {
        let service = test_service(SwitchTlsRoots {
            default_domain: Some("../invalid".into()),
            dns_domain: Some("invalid server name".into()),
            insecure_switch: false,
            ..SwitchTlsRoots::default()
        });

        let response = service
            .handle_batch_disable_switch_mtls(tonic::Request::new(
                rm::BatchDisableSwitchMtlsRequest {
                    nodes: Some(rm::NodeSet {
                        nodes: vec![switch_node_info()],
                    }),
                    services: vec![
                        rm::SwitchService::NvueApi as i32,
                        rm::SwitchService::ScaleUpFabricTelemetryInterface as i32,
                    ],
                },
            ))
            .await
            .unwrap()
            .into_inner();

        let batch = response.response.expect("batch response");

        assert_eq!(batch.status, rm::ReturnCode::Success as i32);
        assert_eq!(batch.stats.expect("batch stats").successful_nodes, 1);
        assert_eq!(batch.node_results.len(), 1);
        assert_eq!(batch.node_results[0].status, rm::ReturnCode::Success as i32);
        assert!(batch.message.contains("GetJobStatus"));

        let parent = service
            .job_tracker
            .get_job(&batch.job_id)
            .expect("parent mTLS disable job");

        let child = service
            .job_tracker
            .get_job(&parent.child_job_ids[0])
            .expect("child mTLS disable job");

        assert_eq!(parent.child_job_ids.len(), 1);
        assert_eq!(child.parent_job_id.as_deref(), Some(batch.job_id.as_str()));
    }

    #[tokio::test]
    async fn batch_disable_switch_mtls_fails_parent_when_child_admission_fails() {
        let tracker = Arc::new(JobTracker::builder().max_tracked_jobs(1).build().unwrap());
        let mut service = test_service(SwitchTlsRoots::default());
        service.job_tracker = Arc::clone(&tracker);

        let response = service
            .handle_batch_disable_switch_mtls(tonic::Request::new(
                rm::BatchDisableSwitchMtlsRequest {
                    nodes: Some(rm::NodeSet {
                        nodes: vec![switch_node_info()],
                    }),
                    services: vec![rm::SwitchService::NvueApi as i32],
                },
            ))
            .await
            .unwrap()
            .into_inner();

        let batch = response.response.expect("batch response");

        assert_eq!(batch.status, rm::ReturnCode::Failure as i32);
        assert!(!batch.job_id.is_empty());
        assert_eq!(batch.node_results.len(), 1);
        assert_eq!(batch.node_results[0].status, rm::ReturnCode::Failure as i32);
        assert_eq!(batch.message, "No switch mTLS disable jobs created");

        assert_eq!(
            tracker.get_job(&batch.job_id).unwrap().state,
            crate::orchestrator::job_lifecycle::JobState::Failed
        );
    }

    #[tokio::test]
    async fn batch_disable_switch_mtls_rejects_empty_services() {
        let service = test_service(SwitchTlsRoots::default());

        let error = service
            .handle_batch_disable_switch_mtls(tonic::Request::new(
                rm::BatchDisableSwitchMtlsRequest {
                    nodes: Some(rm::NodeSet {
                        nodes: vec![switch_node_info()],
                    }),
                    services: Vec::new(),
                },
            ))
            .await
            .unwrap_err();

        assert_eq!(error.code(), tonic::Code::InvalidArgument);

        assert!(
            error
                .message()
                .contains("services must contain at least one SwitchService")
        );
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
