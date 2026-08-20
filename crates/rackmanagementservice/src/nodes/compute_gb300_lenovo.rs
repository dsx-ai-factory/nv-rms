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

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use secrecy::ExposeSecret;
use serde_json::{Value, json};

use crate::domain::node::{
    FirmwareActivationMode, FirmwareActivationRequest, FirmwareActivationSummary, FirmwareInfo,
    FirmwareTarget, FirmwareTaskStatus, FirmwareUpdateOptions, FirmwareUpdateOutcome, Node,
    NodeType, PowerOp, PowerState, PowerTargetType,
};
use crate::domain::rack::NodeConfig;
use crate::nodes::compute_gb300_nvidia::NvidiaGb300Compute;
use crate::transport::http_client::HttpClient;
use crate::transport::redfish_client::NvidiaMnnvlinkTopology;
use crate::utilities::error::{Result, RmsError};

/// Lenovo GB300 compute node.
///
/// Lenovo GB300 compute currently shares the NVIDIA GB300/GB200-backed Redfish,
/// power, and NVFWUPD behavior. This wrapper keeps the concrete node type
/// distinct for API routing and inventory.
pub struct LenovoGb300Compute {
    inner: NvidiaGb300Compute,
    bmc_http: HttpClient,
}

impl LenovoGb300Compute {
    pub fn from_config(config: &NodeConfig, rack_id: &str) -> Result<Self> {
        if config.node_type != NodeType::ComputeGb300Lenovo {
            return Err(RmsError::unimplemented(
                "create_node",
                config.node_type.as_str(),
            ));
        }

        let mut nvidia_config = config.clone();
        nvidia_config.node_type = NodeType::ComputeGb300Nvidia;
        let inner = NvidiaGb300Compute::from_config(&nvidia_config, rack_id)?;
        let bmc_http = lenovo_bmc_http_from_config(config)?;
        Ok(Self { inner, bmc_http })
    }

    pub async fn get_mnnvlink_topology(&self) -> Result<NvidiaMnnvlinkTopology> {
        self.inner.get_mnnvlink_topology().await
    }

    async fn preserve_bmc_update_configuration(&self) -> Result<()> {
        apply_lenovo_bmc_preserve_config(&self.bmc_http, self.id()).await
    }
}

const LENOVO_UPDATE_SERVICE_ENDPOINT: &str = "/redfish/v1/UpdateService";
const LENOVO_AMI_UPDATE_SERVICE_TYPE: &str = "#AMIUpdateService.v1_0_0.AMIUpdateService";
const LENOVO_BMC_INVENTORY_TARGET: &str = "/redfish/v1/UpdateService/FirmwareInventory/BMC";
const LENOVO_BMC_PRESERVE_VERIFY_ATTEMPTS: usize = 5;
#[cfg(not(test))]
const LENOVO_BMC_PRESERVE_VERIFY_RETRY_DELAY: Duration = Duration::from_secs(10);
#[cfg(test)]
const LENOVO_BMC_PRESERVE_VERIFY_RETRY_DELAY: Duration = Duration::from_secs(0);
#[cfg(not(test))]
const LENOVO_BMC_PRESERVE_VERIFY_READBACK_DELAY: Duration = Duration::from_secs(1);
#[cfg(test)]
const LENOVO_BMC_PRESERVE_VERIFY_READBACK_DELAY: Duration = Duration::from_secs(0);

const LENOVO_BMC_FULL_RETAIN_CONFIG: &[(&str, bool)] = &[
    ("Syslog", true),
    ("NTP", true),
    ("Network", true),
    ("Authentication", true),
    ("EXTLOG", true),
    ("FRU", true),
    ("IPMI", true),
    ("KVM", true),
    ("REDFISH", true),
    ("SDR", false),
    ("SEL", true),
    ("SNMP", true),
    ("SSH", true),
    ("WEB", true),
];

fn lenovo_bmc_http_from_config(config: &NodeConfig) -> Result<HttpClient> {
    let Some(bmc_endpoint) = config.bmc_endpoint.as_ref() else {
        return Err(RmsError::invalid_argument(
            "bmc_endpoint is required for Lenovo GB300 compute nodes",
        ));
    };
    let credentials = bmc_endpoint.credentials.as_ref();
    let username = credentials
        .map(|credentials| credentials.username.as_str())
        .unwrap_or_default();
    let password = credentials
        .map(|credentials| credentials.password.expose_secret())
        .unwrap_or_default();

    HttpClient::new(
        &bmc_endpoint.endpoint.ip_address,
        bmc_endpoint.endpoint.port,
        username,
        password,
        bmc_endpoint.dangerously_accept_invalid_certs,
        true,
    )
}

async fn apply_lenovo_bmc_preserve_config(http: &HttpClient, node_id: &str) -> Result<()> {
    tracing::info!(node = %node_id, "preserving Lenovo BMC update configuration");
    for attempt in 1..=LENOVO_BMC_PRESERVE_VERIFY_ATTEMPTS {
        match patch_and_verify_lenovo_bmc_preserve_config(http, node_id).await {
            Ok(()) => {
                tracing::info!(
                    node = %node_id,
                    attempt,
                    "verified Lenovo BMC update configuration preservation"
                );
                return Ok(());
            }
            Err(error) if attempt < LENOVO_BMC_PRESERVE_VERIFY_ATTEMPTS => {
                tracing::warn!(
                    node = %node_id,
                    attempt,
                    attempts = LENOVO_BMC_PRESERVE_VERIFY_ATTEMPTS,
                    retry_delay_secs = LENOVO_BMC_PRESERVE_VERIFY_RETRY_DELAY.as_secs(),
                    error = %error,
                    "Lenovo BMC preserve configuration did not verify; retrying"
                );
                tokio::time::sleep(LENOVO_BMC_PRESERVE_VERIFY_RETRY_DELAY).await;
            }
            Err(error) => return Err(error),
        }
    }

    Err(RmsError::internal(
        "Lenovo BMC preserve configuration did not verify",
    ))
}

async fn patch_and_verify_lenovo_bmc_preserve_config(
    http: &HttpClient,
    node_id: &str,
) -> Result<()> {
    let update_service = http
        .get(LENOVO_UPDATE_SERVICE_ENDPOINT, HttpClient::DEFAULT_TIMEOUT)
        .await?;

    let mut preserve_config = update_service
        .pointer("/Oem/AMIUpdateService/PreserveConfiguration")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    for (key, retain) in LENOVO_BMC_FULL_RETAIN_CONFIG {
        preserve_config.insert((*key).to_owned(), Value::Bool(*retain));
    }

    let payload = json!({
        "Oem": {
            "AMIUpdateService": {
                "@odata.type": LENOVO_AMI_UPDATE_SERVICE_TYPE,
                "PreserveConfiguration": preserve_config,
            }
        }
    });

    http.patch_with_if_match(
        LENOVO_UPDATE_SERVICE_ENDPOINT,
        &payload,
        "*",
        HttpClient::DEFAULT_TIMEOUT,
    )
    .await?;

    tokio::time::sleep(LENOVO_BMC_PRESERVE_VERIFY_READBACK_DELAY).await;
    let verified_update_service = http
        .get(LENOVO_UPDATE_SERVICE_ENDPOINT, HttpClient::DEFAULT_TIMEOUT)
        .await?;
    if let Some(mismatch) = lenovo_bmc_preserve_config_mismatch(&verified_update_service) {
        return Err(RmsError::internal(format!(
            "Lenovo BMC preserve configuration did not verify for {node_id}: {mismatch}"
        )));
    }

    Ok(())
}

fn lenovo_bmc_preserve_config_mismatch(update_service: &Value) -> Option<String> {
    let Some(preserve_config) = update_service
        .pointer("/Oem/AMIUpdateService/PreserveConfiguration")
        .and_then(Value::as_object)
    else {
        return Some("missing Oem.AMIUpdateService.PreserveConfiguration".to_owned());
    };

    for (key, expected) in LENOVO_BMC_FULL_RETAIN_CONFIG {
        match preserve_config.get(*key).and_then(Value::as_bool) {
            Some(actual) if actual == *expected => {}
            Some(actual) => return Some(format!("{key}={actual}, expected {expected}")),
            None => return Some(format!("{key} missing, expected {expected}")),
        }
    }

    None
}

fn lenovo_hgx_update_options(mut options: FirmwareUpdateOptions) -> FirmwareUpdateOptions {
    options.special_json = Some(json!({"Targets": []}));
    options.oem_parameters = Some(json!({"ImageType": "PLDM", "Platform": "HGX"}));
    options
}

fn lenovo_bmc_update_options(
    target: &FirmwareTarget,
    mut options: FirmwareUpdateOptions,
) -> FirmwareUpdateOptions {
    options.special_json = Some(json!({"Targets": [LENOVO_BMC_INVENTORY_TARGET]}));
    options.oem_parameters = Some(json!({"ImageType": lenovo_bmc_image_type(target)}));
    options
}

fn lenovo_update_options_for_target(
    target: &FirmwareTarget,
    options: FirmwareUpdateOptions,
) -> FirmwareUpdateOptions {
    if is_lenovo_hgx_target(target) {
        lenovo_hgx_update_options(options)
    } else if is_lenovo_bmc_target(target) {
        lenovo_bmc_update_options(target, options)
    } else {
        options
    }
}

fn lenovo_version_check_targets(targets: &[FirmwareTarget]) -> Vec<FirmwareTarget> {
    targets
        .iter()
        .cloned()
        .map(|mut target| {
            if is_lenovo_bmc_target(&target) {
                target.component = LENOVO_BMC_INVENTORY_TARGET.to_owned();
            } else if is_lenovo_hgx_target(&target) {
                target.component.clear();
            }
            target
        })
        .collect()
}

fn is_lenovo_hgx_target(target: &FirmwareTarget) -> bool {
    // Firmware-object apply resolves logical HMC to this shared HGX target URI.
    const HGX_HMC_TARGET: &str = "/redfish/v1/Chassis/HGX_Chassis_0";

    let component = target.component.trim();
    component.eq_ignore_ascii_case("HMC") || component.eq_ignore_ascii_case(HGX_HMC_TARGET)
}

fn is_lenovo_bmc_target(target: &FirmwareTarget) -> bool {
    let component = target.component.trim();
    component.eq_ignore_ascii_case("BMC")
        || component.eq_ignore_ascii_case(LENOVO_BMC_INVENTORY_TARGET)
        || (component.is_empty() && firmware_file_is_bmc_image(&target.firmware_file))
}

fn firmware_file_is_bmc_image(firmware_file: &str) -> bool {
    let lower = firmware_file.to_ascii_lowercase();
    lower.ends_with(".ima") || lower.ends_with(".hpm")
}

fn lenovo_bmc_image_type(target: &FirmwareTarget) -> &'static str {
    if target.firmware_file.to_ascii_lowercase().ends_with(".hpm") {
        "HPM"
    } else {
        "BMC"
    }
}

#[async_trait]
impl Node for LenovoGb300Compute {
    fn id(&self) -> &str {
        self.inner.id()
    }

    fn rack_id(&self) -> &str {
        self.inner.rack_id()
    }

    fn node_type(&self) -> NodeType {
        NodeType::ComputeGb300Lenovo
    }

    fn get_info(&self) -> HashMap<String, String> {
        let mut info = self.inner.get_info();
        info.insert(
            "type".to_owned(),
            NodeType::ComputeGb300Lenovo.as_str().to_owned(),
        );
        info
    }

    fn expected_inventory_policy(&self) -> Option<&crate::domain::node::ExpectedInventoryPolicy> {
        self.inner.expected_inventory_policy()
    }

    async fn get_power_state(&self) -> Result<PowerState> {
        self.inner.get_power_state().await
    }

    async fn set_power_state(&self, op: PowerOp, target: PowerTargetType) -> Result<()> {
        self.inner.set_power_state(op, target).await
    }

    async fn get_firmware_inventory(&self) -> Result<Vec<FirmwareInfo>> {
        self.inner.get_firmware_inventory().await
    }

    async fn verify_firmware_package_versions(
        &self,
        targets: &[FirmwareTarget],
    ) -> Result<crate::domain::node::FirmwareVersionCheckSummary> {
        let targets = lenovo_version_check_targets(targets);
        self.inner.verify_firmware_package_versions(&targets).await
    }

    async fn update_firmware(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
        options: FirmwareUpdateOptions,
    ) -> Result<FirmwareUpdateOutcome> {
        let should_preserve_bmc_config = is_lenovo_bmc_target(target);
        let options = lenovo_update_options_for_target(target, options);

        if should_preserve_bmc_config {
            return self
                .inner
                .update_firmware_with_preparation(
                    target,
                    force_update,
                    options,
                    self.preserve_bmc_update_configuration(),
                )
                .await;
        }

        self.inner
            .update_firmware(target, force_update, options)
            .await
    }

    async fn start_firmware_upload(
        &self,
        target: &FirmwareTarget,
        force_update: bool,
    ) -> Result<String> {
        match self
            .update_firmware(target, force_update, FirmwareUpdateOptions::default())
            .await?
        {
            FirmwareUpdateOutcome::Started(handle) => Ok(handle.task_id),
            FirmwareUpdateOutcome::Completed(summary) => Err(RmsError::internal(format!(
                "GB300 Lenovo firmware update completed without task id: {}",
                summary.message
            ))),
            FirmwareUpdateOutcome::Skipped { reason } => Err(RmsError::already_exists(reason)),
        }
    }

    async fn poll_firmware_task(&self, task_id: &str) -> Result<FirmwareTaskStatus> {
        self.inner.poll_firmware_task(task_id).await
    }

    async fn activate_firmware_with(
        &self,
        request: FirmwareActivationRequest,
    ) -> Result<FirmwareActivationSummary> {
        self.inner.activate_firmware_with(request).await
    }

    async fn activate_firmware(&self) -> Result<()> {
        self.activate_firmware_with(FirmwareActivationRequest {
            mode: FirmwareActivationMode::FullGb200Compute,
            cancellation: None,
        })
        .await
        .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::rack::{Endpoint, EndpointConfig, EndpointCredentials};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::{body_json, header, method, path};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    struct SequentialResponder {
        count: Arc<AtomicUsize>,
        responses: Vec<ResponseTemplate>,
    }

    impl SequentialResponder {
        fn json(count: Arc<AtomicUsize>, bodies: Vec<Value>) -> Self {
            let responses = bodies
                .into_iter()
                .map(|body| ResponseTemplate::new(200).set_body_json(body))
                .collect();

            Self { count, responses }
        }
    }

    impl Respond for SequentialResponder {
        fn respond(&self, _request: &Request) -> ResponseTemplate {
            let index = self.count.fetch_add(1, Ordering::SeqCst);
            self.responses
                .get(index)
                .or_else(|| self.responses.last())
                .cloned()
                .unwrap_or_else(|| ResponseTemplate::new(500))
        }
    }

    #[test]
    fn from_config_reports_lenovo_type() {
        let config = NodeConfig {
            id: "c-01".into(),
            node_type: NodeType::ComputeGb300Lenovo,
            bmc_endpoint: Some(EndpointConfig::with_credentials(
                Endpoint {
                    ip_address: "10.0.0.1".into(),
                    mac_address: "aa:bb:cc:dd:ee:ff".into(),
                    port: 8443,
                    host_name: None,
                },
                Some(EndpointCredentials::new("admin", "secret")),
                true,
            )),
            host_endpoint: None,
            expected_inventory: None,
        };

        let compute = LenovoGb300Compute::from_config(&config, "rack-01").unwrap();

        assert_eq!(compute.node_type(), NodeType::ComputeGb300Lenovo);
        assert_eq!(
            compute.get_info()["type"],
            NodeType::ComputeGb300Lenovo.as_str()
        );
    }

    #[test]
    fn lenovo_hgx_target_uses_empty_targets_and_oem_parameters() {
        let target = FirmwareTarget {
            component: "/redfish/v1/Chassis/HGX_Chassis_0".to_owned(),
            firmware_file: "nosbios.fwpkg".to_owned(),
            expected_version: None,
        };
        let options = lenovo_update_options_for_target(&target, FirmwareUpdateOptions::default());

        assert_eq!(options.special_json, Some(json!({"Targets": []})));
        assert_eq!(
            options.oem_parameters,
            Some(json!({"ImageType": "PLDM", "Platform": "HGX"}))
        );
    }

    #[test]
    fn lenovo_bmc_ima_target_uses_bmc_oem_parameters() {
        let target = FirmwareTarget {
            component: "BMC".to_owned(),
            firmware_file: "bmc.ima".to_owned(),
            expected_version: None,
        };
        let options = lenovo_update_options_for_target(&target, FirmwareUpdateOptions::default());

        assert_eq!(
            options.special_json,
            Some(json!({"Targets": [LENOVO_BMC_INVENTORY_TARGET]}))
        );
        assert_eq!(options.oem_parameters, Some(json!({"ImageType": "BMC"})));
    }

    #[test]
    fn lenovo_bmc_hpm_target_uses_hpm_oem_parameters() {
        let target = FirmwareTarget {
            component: "BMC".to_owned(),
            firmware_file: "bmc.hpm".to_owned(),
            expected_version: None,
        };
        let options = lenovo_update_options_for_target(&target, FirmwareUpdateOptions::default());

        assert_eq!(
            options.special_json,
            Some(json!({"Targets": [LENOVO_BMC_INVENTORY_TARGET]}))
        );
        assert_eq!(options.oem_parameters, Some(json!({"ImageType": "HPM"})));
    }

    #[test]
    fn lenovo_empty_bmc_image_target_uses_bmc_oem_parameters() {
        let target = FirmwareTarget {
            component: String::new(),
            firmware_file: "/tmp/Lenovo_NapaC_BMC_v03.03.0_signed.ima".to_owned(),
            expected_version: None,
        };
        let options = lenovo_update_options_for_target(&target, FirmwareUpdateOptions::default());

        assert_eq!(
            options.special_json,
            Some(json!({"Targets": [LENOVO_BMC_INVENTORY_TARGET]}))
        );
        assert_eq!(options.oem_parameters, Some(json!({"ImageType": "BMC"})));
    }

    #[test]
    fn lenovo_empty_non_bmc_image_target_keeps_default_options() {
        let target = FirmwareTarget {
            component: String::new(),
            firmware_file: "/tmp/platform.bin".to_owned(),
            expected_version: None,
        };
        let options = lenovo_update_options_for_target(&target, FirmwareUpdateOptions::default());

        assert_eq!(options.special_json, None);
        assert_eq!(options.oem_parameters, None);
    }

    #[test]
    fn lenovo_version_check_uses_the_actual_update_scope() {
        let targets = vec![
            FirmwareTarget {
                component: "BMC".to_owned(),
                firmware_file: "bmc.ima".to_owned(),
                expected_version: None,
            },
            FirmwareTarget {
                component: "HMC".to_owned(),
                firmware_file: "hgx.fwpkg".to_owned(),
                expected_version: None,
            },
        ];

        let targets = lenovo_version_check_targets(&targets);

        assert_eq!(targets[0].component, LENOVO_BMC_INVENTORY_TARGET);
        assert!(targets[1].component.is_empty());
    }

    #[tokio::test]
    async fn lenovo_bmc_preserve_config_merges_full_retain_and_uses_if_match() {
        let server = MockServer::start().await;
        let get_count = Arc::new(AtomicUsize::new(0));
        let update_service = json!({
            "Oem": {
                "AMIUpdateService": {
                    "PreserveConfiguration": {
                        "Authentication": false,
                        "Network": false,
                        "CustomRetainKey": true
                    }
                }
            }
        });
        let expected_payload = json!({
            "Oem": {
                "AMIUpdateService": {
                    "@odata.type": LENOVO_AMI_UPDATE_SERVICE_TYPE,
                    "PreserveConfiguration": {
                        "Syslog": true,
                        "NTP": true,
                        "Network": true,
                        "Authentication": true,
                        "EXTLOG": true,
                        "FRU": true,
                        "IPMI": true,
                        "KVM": true,
                        "REDFISH": true,
                        "SDR": false,
                        "SEL": true,
                        "SNMP": true,
                        "SSH": true,
                        "WEB": true,
                        "CustomRetainKey": true
                    }
                }
            }
        });
        let verified_update_service = expected_payload.clone();

        Mock::given(method("GET"))
            .and(path(LENOVO_UPDATE_SERVICE_ENDPOINT))
            .respond_with(SequentialResponder::json(
                get_count.clone(),
                vec![update_service, verified_update_service],
            ))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("PATCH"))
            .and(path(LENOVO_UPDATE_SERVICE_ENDPOINT))
            .and(header("If-Match", "*"))
            .and(body_json(&expected_payload))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;

        let http = HttpClient::for_test(&server.uri());
        apply_lenovo_bmc_preserve_config(&http, "compute-01")
            .await
            .unwrap();
        assert_eq!(get_count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn lenovo_bmc_preserve_config_retries_until_readback_matches() {
        let server = MockServer::start().await;
        let get_count = Arc::new(AtomicUsize::new(0));
        let update_service = json!({
            "Oem": {
                "AMIUpdateService": {
                    "PreserveConfiguration": {
                        "Authentication": false,
                        "Network": false
                    }
                }
            }
        });
        let expected_payload = json!({
            "Oem": {
                "AMIUpdateService": {
                    "@odata.type": LENOVO_AMI_UPDATE_SERVICE_TYPE,
                    "PreserveConfiguration": {
                        "Syslog": true,
                        "NTP": true,
                        "Network": true,
                        "Authentication": true,
                        "EXTLOG": true,
                        "FRU": true,
                        "IPMI": true,
                        "KVM": true,
                        "REDFISH": true,
                        "SDR": false,
                        "SEL": true,
                        "SNMP": true,
                        "SSH": true,
                        "WEB": true
                    }
                }
            }
        });
        let stale_readback = json!({
            "Oem": {
                "AMIUpdateService": {
                    "PreserveConfiguration": {
                        "Authentication": false,
                        "Network": false
                    }
                }
            }
        });

        Mock::given(method("GET"))
            .and(path(LENOVO_UPDATE_SERVICE_ENDPOINT))
            .respond_with(SequentialResponder::json(
                get_count.clone(),
                vec![
                    update_service.clone(),
                    stale_readback,
                    update_service,
                    expected_payload.clone(),
                ],
            ))
            .expect(4)
            .mount(&server)
            .await;
        Mock::given(method("PATCH"))
            .and(path(LENOVO_UPDATE_SERVICE_ENDPOINT))
            .and(header("If-Match", "*"))
            .and(body_json(&expected_payload))
            .respond_with(ResponseTemplate::new(204))
            .expect(2)
            .mount(&server)
            .await;

        let http = HttpClient::for_test(&server.uri());
        apply_lenovo_bmc_preserve_config(&http, "compute-01")
            .await
            .unwrap();
        assert_eq!(get_count.load(Ordering::SeqCst), 4);
    }
}
