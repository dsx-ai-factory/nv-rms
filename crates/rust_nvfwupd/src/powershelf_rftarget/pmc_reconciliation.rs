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

//! Read-only reconciliation for the legacy LiteOn PMC's lost upload response.
//!
//! FirmwareInventory exposes staged images as Enabled, even while the running
//! manager reports `deadbeef`. Neither field establishes readiness to reboot.

use std::collections::BTreeMap;

use serde_json::{json, Value};
use tokio::time::{sleep, timeout, Duration};

use super::PowerShelfRFTarget;

const MANAGER: &str = "/redfish/v1/Managers/bmc";
const INVENTORY: &str = "/redfish/v1/UpdateService/FirmwareInventory";

#[derive(Clone, Debug)]
pub(super) struct PmcSnapshot {
    serial: String,
    version: String,
    active_image: Option<String>,
    images: BTreeMap<String, Value>,
}

impl PmcSnapshot {
    fn reconcile(&self, after: &Self) -> Value {
        if self.serial != after.serial {
            return json!({"state": "identity_changed", "activation_ready": false});
        }
        let candidates: Vec<_> = after
            .images
            .iter()
            .filter(|(uri, image)| self.images.get(*uri) != Some(*image))
            .map(|(uri, image)| json!({"uri": uri, "image": image}))
            .collect();
        json!({
            "state": "activation_readiness_unproven",
            "activation_ready": false,
            "manager_version_before": self.version,
            "manager_version_after": after.version,
            "active_image_before": self.active_image,
            "active_image_after": after.active_image,
            "new_or_changed_images": candidates,
            "action": "Stop; do not replay the upload or reboot based on inventory alone"
        })
    }
}

impl PowerShelfRFTarget {
    /// A complete bounded baseline, restricted to the observed GB200 LiteOn PMC.
    /// Missing inventory, identity, or unsafe links are unknown, never empty.
    pub(super) async fn legacy_pmc_snapshot(&self) -> Option<PmcSnapshot> {
        timeout(Duration::from_secs(20), async {
            let manager = self.get_chassis_json(MANAGER).await?;
            if manager["Model"] != "CM-14MP-1R" || manager["HardwareVersion"] != "X6" {
                return None;
            }
            let serial = manager["SerialNumber"].as_str()?.trim();
            if serial.is_empty() {
                return None;
            }
            let inventory = self.get_chassis_json(INVENTORY).await?;
            let members = inventory["Members"].as_array()?;
            // This PMC has very few images. Refuse incomplete/paginated lists.
            if members.len() > 16
                || inventory["Members@odata.count"].as_u64()? != members.len() as u64
                || inventory.get("Members@odata.nextLink").is_some()
            {
                return None;
            }
            let mut images = BTreeMap::new();
            for member in members {
                let uri = Self::same_origin_redfish_path(
                    &self.bmc_access.base_url,
                    member["@odata.id"].as_str()?,
                    "PMC firmware inventory member",
                )
                .ok()?;
                if !uri.starts_with(&format!("{INVENTORY}/")) {
                    return None;
                }
                let image = self.get_chassis_json(&uri).await?;
                if image["RelatedItem"]
                    .as_array()
                    .is_some_and(|related| related.iter().any(|item| item["@odata.id"] == MANAGER))
                {
                    image["Version"].as_str().filter(|v| !v.trim().is_empty())?;
                    // Keep only meaningful fields so timestamps cannot turn an
                    // unchanged old image into evidence for the current upload.
                    images.insert(
                        uri,
                        json!({"version": image["Version"], "status": image["Status"]}),
                    );
                }
            }
            Some(PmcSnapshot {
                serial: serial.to_owned(),
                version: manager["FirmwareVersion"].as_str()?.to_owned(),
                active_image: manager["Links"]["ActiveSoftwareImage"]["@odata.id"]
                    .as_str()
                    .map(str::to_owned),
                images,
            })
        })
        .await
        .ok()
        .flatten()
    }

    /// Reconcile a missing task without SSH, an upload retry, or a reset.
    pub(super) async fn reconcile_legacy_pmc_upload(&self, before: &PmcSnapshot) -> Value {
        // bmcweb may be restarting after a crash. Bound all reads and retries.
        for attempt in 0..3 {
            if attempt > 0 {
                #[cfg(not(test))]
                sleep(Duration::from_secs(2)).await;
                #[cfg(test)]
                sleep(Duration::ZERO).await;
            }
            if let Some(after) = self.legacy_pmc_snapshot().await {
                return before.reconcile(&after);
            }
        }
        json!({"state": "inventory_unavailable", "activation_ready": false})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bmc_access::BmcAccess;
    use crate::rf_target::{CmdArgs, RFTarget};
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, ResponseTemplate,
    };

    fn baseline() -> PmcSnapshot {
        PmcSnapshot {
            serial: "shelf-a".into(),
            version: "r1.3.9".into(),
            active_image: Some(format!("{INVENTORY}/pmc_old")),
            images: BTreeMap::from([(
                format!("{INVENTORY}/pmc_old"),
                json!({"version": "r1.3.9", "status": {"State": "Enabled"}}),
            )]),
        }
    }

    #[test]
    fn staged_inventory_and_deadbeef_do_not_authorize_activation() {
        let before = baseline();
        let mut after = before.clone();
        after.version = "deadbeef".into();
        after.active_image = None;
        after.images = BTreeMap::from([(
            format!("{INVENTORY}/68b2b344"),
            json!({"version": "r1.3.8-rc1", "status": {"State": "Enabled"}}),
        )]);
        let evidence = before.reconcile(&after);
        assert_eq!(evidence["activation_ready"], false);
        assert_eq!(
            evidence["new_or_changed_images"].as_array().unwrap().len(),
            1
        );
        assert_eq!(evidence["state"], "activation_readiness_unproven");
    }

    #[test]
    fn stale_inventory_is_not_attributed_to_the_upload() {
        let before = baseline();
        let evidence = before.reconcile(&before);
        assert_eq!(evidence["new_or_changed_images"], json!([]));
        assert_eq!(evidence["activation_ready"], false);
    }

    #[test]
    fn changed_device_identity_is_rejected() {
        let before = baseline();
        let mut after = before.clone();
        after.serial = "different-shelf".into();
        assert_eq!(before.reconcile(&after)["state"], "identity_changed");
    }

    async fn missing_task_stops_without_reset_or_reupload(
        upload_status: u16,
        task_read_fails: bool,
    ) {
        let server = MockServer::start().await;
        let uploaded = Arc::new(AtomicBool::new(false));
        let state = uploaded.clone();
        Mock::given(method("GET")).and(path(MANAGER))
            .respond_with(move |_: &wiremock::Request| {
                let staged = state.load(Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_json(json!({
                    "Model": "CM-14MP-1R", "HardwareVersion": "X6", "SerialNumber": "shelf-a",
                    "FirmwareVersion": if staged {"deadbeef"} else {"r1.3.9"},
                    "Links": if staged {json!({})} else {json!({"ActiveSoftwareImage": {"@odata.id": format!("{INVENTORY}/old")}})}
                }))
            }).expect(2).mount(&server).await;
        let state = uploaded.clone();
        Mock::given(method("GET")).and(path(INVENTORY))
            .respond_with(move |_: &wiremock::Request| {
                let id = if state.load(Ordering::SeqCst) {"new"} else {"old"};
                ResponseTemplate::new(200).set_body_json(json!({
                    "Members@odata.count": 2,
                    "Members": [{"@odata.id": format!("{INVENTORY}/{id}")}, {"@odata.id": format!("{INVENTORY}/mcu")}]
                }))
            }).mount(&server).await;
        for (id, version) in [("old", "r1.3.9"), ("new", "r1.3.8-rc1")] {
            Mock::given(method("GET")).and(path(format!("{INVENTORY}/{id}")))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "Version": version, "RelatedItem": [{"@odata.id": MANAGER}], "Status": {"State": "Enabled"}
                }))).expect(1).mount(&server).await;
        }
        Mock::given(method("GET"))
            .and(path(format!("{INVENTORY}/mcu")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"Version":"null"})))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/TaskService/Tasks"))
            .respond_with(
                ResponseTemplate::new(if task_read_fails { 503 } else { 200 })
                    .set_body_json(json!({"Members":[]})),
            )
            .expect(if task_read_fails { 1 } else { 2 })
            .mount(&server)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/redfish/v1/UpdateService"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/redfish/v1/UpdateService/update"))
            .respond_with(move |_: &wiremock::Request| {
                uploaded.store(true, Ordering::SeqCst);
                ResponseTemplate::new(upload_status).set_body_json(json!({"message":"no task"}))
            })
            .expect(1)
            .mount(&server)
            .await;
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("cm14mp1r-r1.3.8-rc1.tar");
        tokio::fs::write(&file, b"pmc firmware").await.unwrap();
        let mut target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            None,
        );
        let mut output = json!({"Output": [], "Error": [], "Error Code": 0});
        let args = CmdArgs {
            cmd: "update_fw".into(),
            background: false,
            details: false,
            staged_update: false,
            staged_activate_update: false,
            quiet: true,
            oem_parameters: None,
            special: Some(vec![json!({"ApplyTime":"OnReset"}).to_string()]),
        };
        let task = target
            .update_component(
                &args,
                "/redfish/v1/UpdateService/update",
                file.to_str().unwrap(),
                30,
                Some(&mut output),
                true,
            )
            .await;
        assert!(task.is_none());
        assert_eq!(output["Error Code"], 1);
        assert_eq!(output["Output"][0]["recovery"]["activation_ready"], false);
        assert_eq!(
            output["Output"][0]["recovery"]["new_or_changed_images"][0]["image"]["version"],
            "r1.3.8-rc1"
        );
        let requests = server.received_requests().await.unwrap();
        let posts: Vec<_> = requests
            .iter()
            .filter(|r| r.method == wiremock::http::Method::Post)
            .collect();
        assert_eq!(posts.len(), 1, "must neither reboot nor replay the upload");
        assert_eq!(posts[0].url.path(), "/redfish/v1/UpdateService/update");
    }

    #[tokio::test]
    async fn upload_error_with_staged_image_stops() {
        missing_task_stops_without_reset_or_reupload(500, false).await;
    }

    #[tokio::test]
    async fn upload_success_without_task_is_not_firmware_success() {
        missing_task_stops_without_reset_or_reupload(200, false).await;
    }

    #[tokio::test]
    async fn failed_task_baseline_cannot_be_used_to_recover_a_task() {
        missing_task_stops_without_reset_or_reupload(500, true).await;
    }

    #[tokio::test]
    async fn task_baseline_includes_tasks_not_yet_at_handoff() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/TaskService/Tasks"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                json!({"Members":[{"@odata.id":"/redfish/v1/TaskService/Tasks/old"}]}),
            ))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/redfish/v1/TaskService/Tasks/old"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"TaskState":"Running", "PercentComplete":1})),
            )
            .mount(&server)
            .await;
        let target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            None,
        );
        let tasks = target.on_reset_handoff_tasks(false).await.unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].0, "old");
        assert!(target
            .on_reset_handoff_tasks(true)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn incomplete_or_unsafe_inventory_is_unknown() {
        for inventory in [
            json!({"Members":[], "Members@odata.count":1}),
            json!({"Members":[], "Members@odata.count":0, "Members@odata.nextLink":"/next"}),
            json!({"Members":[{"@odata.id":"https://other-host/redfish/v1/UpdateService/FirmwareInventory/x"}], "Members@odata.count":1}),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("GET")).and(path(MANAGER))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({"Model":"CM-14MP-1R", "HardwareVersion":"X6", "SerialNumber":"a", "FirmwareVersion":"r1.3.9"})))
                .mount(&server).await;
            Mock::given(method("GET"))
                .and(path(INVENTORY))
                .respond_with(ResponseTemplate::new(200).set_body_json(inventory))
                .mount(&server)
                .await;
            let target = PowerShelfRFTarget::new(
                BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
                None,
            );
            assert!(target.legacy_pmc_snapshot().await.is_none());
            assert_eq!(server.received_requests().await.unwrap().len(), 2);
        }
    }

    #[tokio::test]
    async fn unavailable_inventory_remains_unknown() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(MANAGER))
            .respond_with(ResponseTemplate::new(503))
            .expect(3)
            .mount(&server)
            .await;
        let target = PowerShelfRFTarget::new(
            BmcAccess::mock_with_base_url(server.uri(), "mock-powershelf"),
            None,
        );
        let result = target.reconcile_legacy_pmc_upload(&baseline()).await;
        assert_eq!(result["state"], "inventory_unavailable");
        assert_eq!(result["activation_ready"], false);
    }
}
