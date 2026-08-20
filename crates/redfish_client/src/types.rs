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
