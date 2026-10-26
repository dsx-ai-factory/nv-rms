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

use nv_redfish::core::ModificationResponse;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub(crate) struct RedfishTaskResponse {
    #[serde(rename = "Id")]
    id: Option<String>,

    #[serde(rename = "@odata.id")]
    odata_id: Option<String>,
}

pub(crate) fn task_id_from_modification_response(
    response: ModificationResponse<RedfishTaskResponse>,
) -> Option<String> {
    match response {
        ModificationResponse::Entity(response) => match response.id.filter(|id| !id.is_empty()) {
            Some(id) => Some(id),
            None => response
                .odata_id
                .and_then(|uri| task_id_from_odata_id(&uri)),
        },
        ModificationResponse::Task(task) => task_id_from_odata_id(&task.location.0.to_string()),
        ModificationResponse::Empty => None,
    }
}

fn task_id_from_odata_id(uri: &str) -> Option<String> {
    let path = uri_path_without_suffix(uri).trim_end_matches('/');

    let segments = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();

    if segments.len() == 1 {
        return Some(segments[0].to_owned());
    }

    for window in segments.windows(3) {
        if window[0].eq_ignore_ascii_case("TaskService")
            && (window[1].eq_ignore_ascii_case("Tasks")
                || window[1].eq_ignore_ascii_case("TaskMonitors"))
        {
            return Some(window[2].to_owned());
        }
    }

    None
}

fn uri_path_without_suffix(uri: &str) -> &str {
    match [uri.find('?'), uri.find('#')].into_iter().flatten().min() {
        Some(index) => &uri[..index],
        None => uri,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_id_extraction_accepts_only_task_resource_paths() {
        assert_eq!(task_id_from_odata_id(""), None);
        assert_eq!(task_id_from_odata_id("/"), None);

        assert_eq!(
            task_id_from_odata_id("/redfish/v1/TaskService/Tasks/42?monitor=abc"),
            Some("42".to_owned())
        );

        assert_eq!(
            task_id_from_odata_id("/redfish/v1/UpdateService/FirmwareInventory/BMC"),
            None
        );

        assert_eq!(task_id_from_odata_id("/redfish/v1/Oem/Tasks/42"), None);

        assert_eq!(task_id_from_odata_id("43"), Some("43".to_owned()));

        assert_eq!(
            task_id_from_odata_id("/redfish/v1/TaskService/Tasks/42/Monitor"),
            Some("42".to_owned())
        );

        assert_eq!(
            task_id_from_odata_id("/redfish/v1/TaskService/TaskMonitors/42"),
            Some("42".to_owned())
        );

        assert_eq!(
            task_id_from_odata_id(
                "https://bmc.example/redfish/v1/TaskService/Tasks/42/Monitor?token=abc"
            ),
            Some("42".to_owned())
        );
    }

    #[test]
    fn task_id_extraction_rejects_empty_entity_odata_id() {
        let response = ModificationResponse::Entity(RedfishTaskResponse {
            id: None,
            odata_id: Some(String::new()),
        });

        assert_eq!(task_id_from_modification_response(response), None);
    }

    #[test]
    fn task_id_extraction_accepts_entity_monitor_odata_id() {
        let response = ModificationResponse::Entity(RedfishTaskResponse {
            id: None,
            odata_id: Some("/redfish/v1/TaskService/Tasks/42/Monitor?token=abc".to_owned()),
        });

        assert_eq!(
            task_id_from_modification_response(response),
            Some("42".to_owned())
        );
    }

    #[test]
    fn task_id_extraction_prefers_entity_id() {
        let response = ModificationResponse::Entity(RedfishTaskResponse {
            id: Some("Task-7".to_owned()),
            odata_id: Some("/redfish/v1/TaskService/Tasks/ignored".to_owned()),
        });

        assert_eq!(
            task_id_from_modification_response(response),
            Some("Task-7".to_owned())
        );
    }
}
