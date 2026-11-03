/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Physical discovery and independent hardware samples shared with reconciliation.

mod discovery;
mod health;
mod sampling;

pub(super) use sampling::observe_group;

pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) use health::{
    GroupObservation, interpret_group,
};

use std::collections::BTreeMap;
use std::future::Future;

use super::super::EphemeralSwitch;
use super::super::observations::{FabricSamples, ObservedIdentity, sample_group};
use super::planner::ObservedPlacement;
use crate::config::fabric_layout::FabricLinkState;
use crate::nodes::ScaleUpLinkSnapshot;
use crate::orchestrator::job_lifecycle::JobScope;
use crate::utilities::error::RmsError;

use librms::protos::rack_manager as rm;
use nvue_client::DEFAULT_TIMEOUT;
use nvue_client::cluster::ClusterState;
use nvue_client::interface::{InterfaceCollection, InterfaceLinkState};

use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use tokio_util::sync::CancellationToken;

/// Request-scoped switch transport paired with independently observed placement.
pub(super) struct ManagedSwitch {
    /// Caller identity and endpoints used for transport and response correlation.
    pub(super) node: rm::NodeInfo,

    /// Initialized switch clients with the request's security policy.
    pub(super) prepared: EphemeralSwitch,

    /// Hardware identity used for planning and physical admission.
    pub(super) placement: ObservedPlacement,
}

impl ManagedSwitch {
    /// Reads saved cluster state independently from effective controller state.
    pub(super) async fn startup_enabled(&self) -> Result<bool, String> {
        let switch = self.prepared.switch.as_switch_gb200().ok_or_else(|| {
            format!(
                "switch '{}' has no saved cluster adapter",
                self.node.node_id
            )
        })?;

        switch
            .startup_cluster_state()
            .await
            .map(|state| state == ClusterState::Enabled)
            .map_err(|error| {
                format!(
                    "cannot read switch '{}' saved cluster state: {}",
                    self.node.node_id, error.code
                )
            })
    }

    /// Repairs a selected controller's saved enable without restarting a healthy process.
    pub(super) async fn save_enabled_cluster(&self) -> Result<(), String> {
        let switch = self.prepared.switch.as_switch_gb200().ok_or_else(|| {
            format!(
                "switch '{}' has no saved cluster adapter",
                self.node.node_id
            )
        })?;

        let _guard = switch.op_lock.lock().await;

        if !self.enabled().await? {
            return Err(format!(
                "switch '{}' controller is not enabled",
                self.node.node_id
            ));
        }

        if !self.startup_enabled().await? {
            switch.save_current_applied_config().await.map_err(|_| {
                format!(
                    "cannot save switch '{}' cluster configuration",
                    self.node.node_id
                )
            })?;
        }

        if !self.startup_enabled().await? {
            return Err(format!(
                "switch '{}' saved cluster is not enabled",
                self.node.node_id
            ));
        }

        Ok(())
    }

    /// Controller activation is identified by observed chassis and tray, not caller labels.
    pub(super) fn controller_identity(
        &self,
    ) -> crate::orchestrator::fabric_operation::ControllerIdentity {
        crate::orchestrator::fabric_operation::ControllerIdentity {
            chassis_serial_number: self.placement.chassis_serial_number.clone(),
            tray_index: self.placement.position.tray_index,
        }
    }

    /// Includes physical and endpoint aliases so caller labels cannot bypass admission.
    pub(super) fn scopes(&self) -> Result<Vec<JobScope>, String> {
        let client = self.prepared.switch.nvue_client().ok_or_else(|| {
            format!(
                "switch '{}' has no NVUE admission client",
                self.node.node_id
            )
        })?;

        let mut scopes = crate::api::grpc::switch_scope::switch_scopes_for_placement(
            client.endpoint(),
            &self.node.rack_id,
            &self.node.node_id,
            &self.placement.chassis_serial_number,
            self.placement.position.tray_index,
        );

        scopes.extend(
            crate::api::grpc::switch_scope::request_switch_scopes(&self.node).map_err(|_| {
                format!(
                    "switch '{}' has invalid management endpoints",
                    self.node.node_id
                )
            })?,
        );

        Ok(scopes)
    }

    /// Errors and unrecognized states cannot be interpreted as disabled controllers.
    pub(super) async fn enabled(&self) -> Result<bool, String> {
        let client = self
            .prepared
            .switch
            .nvue_client()
            .ok_or_else(|| format!("switch '{}' has no NVUE client", self.node.node_id))?;

        let cluster = client.get_cluster(DEFAULT_TIMEOUT).await.map_err(|error| {
            format!(
                "cannot read switch '{}' cluster state: {}",
                self.node.node_id,
                RmsError::from(error).code
            )
        })?;

        match cluster
            .state
            .as_deref()
            .and_then(|state| ClusterState::try_from(state).ok())
        {
            Some(ClusterState::Enabled) => Ok(true),
            Some(ClusterState::Disabled) => Ok(false),
            _ => Err(format!(
                "switch '{}' reports unknown cluster state",
                self.node.node_id
            )),
        }
    }
}

/// Runs a validated finite device batch concurrently, preserving input order.
///
/// After failure, panic, or cancellation, operations not yet started are skipped.
/// All operations may start before the first failure is observed. Started
/// operations are drained before returning so recovery cannot race a write.
pub(super) async fn run_parallel<I, F, T>(
    operations: I,
    cancellation: Option<&CancellationToken>,
    operation_name: &str,
) -> Result<Vec<T>, String>
where
    I: IntoIterator<Item = F>,
    F: Future<Output = Result<T, String>>,
{
    let stop_dispatch = CancellationToken::new();
    let stopped = &stop_dispatch;

    let mut pending: FuturesUnordered<_> = operations
        .into_iter()
        .enumerate()
        .map(|(index, operation)| async move {
            let result = if stopped.is_cancelled()
                || cancellation.is_some_and(CancellationToken::is_cancelled)
            {
                Err("managed reconciliation cancelled or device phase stopped".to_owned())
            } else {
                std::panic::AssertUnwindSafe(operation)
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| Err(format!("{operation_name} panicked")))
            };

            (index, result)
        })
        .collect();

    let mut results = BTreeMap::new();
    let mut first_error = None;

    while let Some((index, result)) = pending.next().await {
        match result {
            Ok(result) => {
                results.insert(index, result);
            }
            Err(error) => {
                stop_dispatch.cancel();
                first_error.get_or_insert(error);
            }
        }
    }

    if let Some(error) = first_error {
        return Err(error);
    }

    if cancellation.is_some_and(CancellationToken::is_cancelled) {
        return Err("managed reconciliation cancelled".to_owned());
    }

    Ok(results.into_values().collect())
}

/// Distinguishes absent overrides from unknown or ambiguous NVUE state.
pub(in crate::api::grpc::scaleupfabricmanager_handlers::handlers_v2) fn explicit_state(
    collection: &InterfaceCollection,
    id: &str,
) -> Result<Option<InterfaceLinkState>, String> {
    let Some(state) = collection
        .get(id)
        .and_then(|interface| interface.link.state.as_ref())
    else {
        return Ok(None);
    };

    if state.is_empty() {
        return Ok(None);
    }

    if state.len() == 1 && state.contains_key("up") {
        return Ok(Some(InterfaceLinkState::Up));
    }

    if state.len() == 1 && state.contains_key("down") {
        return Ok(Some(InterfaceLinkState::Down));
    }

    Err(format!(
        "interface '{id}' reports unknown or ambiguous administrative state"
    ))
}

/// Requires explicit applied and startup overrides plus matching effective state.
///
/// A missing override does not match an explicit target, even if the device
/// default produces that state. Unknown state fails comparison; physical link
/// health is not checked here.
pub(super) fn links_match(
    snapshot: &ScaleUpLinkSnapshot,
    desired: &BTreeMap<String, FabricLinkState>,
) -> Result<bool, String> {
    let mut matches = true;

    for (id, target) in desired {
        let effective = explicit_state(&snapshot.effective, id)?.ok_or_else(|| {
            format!("interface '{id}' has no observable effective administrative state")
        })?;

        let applied = explicit_state(&snapshot.applied, id)?;
        let startup = explicit_state(&snapshot.startup, id)?;

        let expected = match target {
            FabricLinkState::Up => InterfaceLinkState::Up,
            FabricLinkState::Down => InterfaceLinkState::Down,
        };

        matches &= applied == Some(expected) && startup == Some(expected) && effective == expected;
    }

    Ok(matches)
}

#[cfg(test)]
mod tests;
