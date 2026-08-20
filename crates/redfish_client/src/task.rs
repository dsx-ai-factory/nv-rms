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
