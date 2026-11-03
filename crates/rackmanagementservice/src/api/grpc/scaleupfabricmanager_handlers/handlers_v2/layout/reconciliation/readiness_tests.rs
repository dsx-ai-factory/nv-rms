/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Controller completion is independent of operational data-plane status.

use super::activation::is_controller_ready;
use super::observations;

use librms::protos::rack_manager as rm;

#[test]
fn controller_readiness_does_not_hide_independent_data_plane_health() {
    let mut observation = observations::GroupObservation {
        control_plane_state: rm::ScaleUpFabricControlPlaneState::Configured,
        controller_observed: true,
        errors: Vec::new(),
        health: rm::ScaleUpFabricComponentStatus {
            controller: rm::ScaleUpFabricReportedStatus::Normal as i32,
            compute: rm::ScaleUpFabricReportedStatus::Abnormal as i32,
            switches: rm::ScaleUpFabricReportedStatus::Abnormal as i32,
            links: rm::ScaleUpFabricReportedStatus::Abnormal as i32,
        },
        membership_observed: true,
        membership_matches: true,
    };

    assert!(is_controller_ready(&observation));

    assert_eq!(
        observation.health.links,
        rm::ScaleUpFabricReportedStatus::Abnormal as i32
    );

    for state in [
        rm::ScaleUpFabricReportedStatus::Unknown,
        rm::ScaleUpFabricReportedStatus::Abnormal,
    ] {
        observation.health.controller = state as i32;

        assert!(!is_controller_ready(&observation));
    }

    observation.health.controller = rm::ScaleUpFabricReportedStatus::Normal as i32;

    for state in [
        rm::ScaleUpFabricControlPlaneState::Unknown,
        rm::ScaleUpFabricControlPlaneState::Failed,
    ] {
        observation.control_plane_state = state;

        assert!(!is_controller_ready(&observation));
    }
}
