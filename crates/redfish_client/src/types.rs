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

use serde::{Deserialize, Serialize};

/// NVIDIA MNNVLink topology payload extracted from the Processor OEM block.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct NvidiaMnnvlinkTopology {
    /// Chassis serial number reported by `Oem.Nvidia.MNNVLinkTopology`.
    #[serde(rename(deserialize = "ChassisSerialNumber", serialize = "chassis_sn"))]
    pub chassis_sn: String,

    /// Tray slot number reported by `Oem.Nvidia.MNNVLinkTopology`.
    #[serde(rename(deserialize = "TraySlotNumber", serialize = "slot_number"))]
    pub slot_number: i64,

    /// Tray slot index reported by `Oem.Nvidia.MNNVLinkTopology`.
    #[serde(rename(deserialize = "TraySlotIndex", serialize = "tray_index"))]
    pub tray_index: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    #[test]
    fn nvidia_mnnvlink_topology_preserves_json_shape() {
        let topology: NvidiaMnnvlinkTopology = serde_json::from_value(json!({
            "ChassisSerialNumber": "chassis-01",
            "TraySlotNumber": 7,
            "TraySlotIndex": 2,
        }))
        .unwrap();

        let value = serde_json::to_value(topology).unwrap();

        assert_eq!(
            value,
            json!({
                "chassis_sn": "chassis-01",
                "slot_number": 7,
                "tray_index": 2,
            })
        );
    }
}
