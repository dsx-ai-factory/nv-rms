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

use std::time::Duration;

use crate::domain::node::{Node, PowerState};
use crate::utilities::error::{ErrorCode, Result, RmsError};

/// How long to poll for the node chassis to report `On` after an aux powercycle.
#[cfg(not(test))]
pub(crate) const NODE_AUX_POWERCYCLE_RECOVERY_TIMEOUT: Duration = Duration::from_secs(10 * 60);
#[cfg(test)]
pub(crate) const NODE_AUX_POWERCYCLE_RECOVERY_TIMEOUT: Duration = Duration::from_millis(200);

/// Interval between power-state polls during aux powercycle recovery.
#[cfg(not(test))]
pub(crate) const NODE_AUX_POWERCYCLE_POWER_POLL_INTERVAL: Duration = Duration::from_secs(15);
#[cfg(test)]
pub(crate) const NODE_AUX_POWERCYCLE_POWER_POLL_INTERVAL: Duration = Duration::ZERO;

/// Grace period to wait after the node reports `On` before retrying any action,
/// giving the host enough time to finish booting.
#[cfg(not(test))]
pub(crate) const NODE_AUX_POWERCYCLE_BOOT_GRACE: Duration = Duration::from_secs(30);
#[cfg(test)]
pub(crate) const NODE_AUX_POWERCYCLE_BOOT_GRACE: Duration = Duration::ZERO;

/// Returns `true` when the error indicates that a node's host is
/// unreachable. DNS errors are not considered here, since power-cycle recovery
/// would have no effect for a DNS lookup failure.
pub(crate) fn is_unreachable_err(error: &RmsError) -> bool {
    matches!(
        error.code,
        ErrorCode::ConnectionRefused | ErrorCode::Timeout | ErrorCode::Unavailable
    )
}

/// Issues a BMC aux powercycle, then polls until the node reports
/// `PowerState::On`. If the node successfully returns 'On',
/// it will wait for the boot grace period before returning to the caller.
/// Implementation of BMC powercycle itself is node-specific,
/// and implemented by the concrete node types.
///
/// Returns `Ok(())` when the node is back on and ready.
/// Returns `Err` if the powercycle command itself fails or the power-on timeout
/// is exceeded.
pub(crate) async fn attempt_bmc_aux_powercycle_recovery(node: &dyn Node) -> Result<()> {
    node.bmc_aux_powercycle().await.inspect_err(|e| {
        tracing::error!(
            node = %node.id(),
            error = %e.message,
            "BMC aux powercycle command failed"
        );
    })?;

    tracing::info!(
        node = %node.id(),
        timeout_secs = NODE_AUX_POWERCYCLE_RECOVERY_TIMEOUT.as_secs(),
        "BMC aux powercycle issued; polling for power-on"
    );

    let deadline = tokio::time::Instant::now() + NODE_AUX_POWERCYCLE_RECOVERY_TIMEOUT;
    loop {
        if !NODE_AUX_POWERCYCLE_POWER_POLL_INTERVAL.is_zero() {
            tokio::time::sleep(NODE_AUX_POWERCYCLE_POWER_POLL_INTERVAL).await;
        }

        match node.get_power_state().await {
            Ok(PowerState::On) => {
                tracing::info!(node = %node.id(), "node power is On after aux powercycle");

                // Got 'On' state, but the host may not be fully booted yet.
                // Wait for the boot grace period before returning to the caller.
                if !NODE_AUX_POWERCYCLE_BOOT_GRACE.is_zero() {
                    tracing::info!(
                        node = %node.id(),
                        grace_secs = NODE_AUX_POWERCYCLE_BOOT_GRACE.as_secs(),
                        "waiting grace period for node to settle"
                    );
                    tokio::time::sleep(NODE_AUX_POWERCYCLE_BOOT_GRACE).await;
                }

                return Ok(());
            }
            Ok(state) => {
                tracing::debug!(
                    node = %node.id(),
                    ?state,
                    "node not yet On after aux powercycle"
                );
            }
            Err(e) => {
                tracing::warn!(
                    node = %node.id(),
                    error = %e.message,
                    "get_power_state failed during aux powercycle recovery; continuing to poll"
                );
            }
        }

        if tokio::time::Instant::now() >= deadline {
            tracing::error!(
                node = %node.id(),
                timeout_secs = NODE_AUX_POWERCYCLE_RECOVERY_TIMEOUT.as_secs(),
                "node did not report power-on within timeout after aux powercycle"
            );
            return Err(RmsError::internal(format!(
                "node {} did not report power-on within {}s after aux powercycle",
                node.id(),
                NODE_AUX_POWERCYCLE_RECOVERY_TIMEOUT.as_secs(),
            )));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::domain::node::{NodeKind, NodeType as DomainNodeType, PowerState};
    use crate::utilities::error::{ErrorCode, RmsError};

    struct RecoveryMockNode {
        node_type: DomainNodeType,
        /// Errors returned by `bmc_aux_powercycle`; empty queue → `Ok(())`.
        powercycle_errors: Mutex<VecDeque<RmsError>>,
        /// Power states returned by `get_power_state`; empty queue → `power_state_default`.
        power_states: Mutex<VecDeque<PowerState>>,
        /// Returned by `get_power_state` once the `power_states` queue is exhausted.
        power_state_default: PowerState,
        powercycle_calls: AtomicUsize,
    }

    impl RecoveryMockNode {
        fn switch() -> Self {
            Self {
                node_type: DomainNodeType::SwitchGb200Nvidia,
                powercycle_errors: Mutex::new(VecDeque::new()),
                power_states: Mutex::new(VecDeque::new()),
                power_state_default: PowerState::On,
                powercycle_calls: AtomicUsize::new(0),
            }
        }

        fn with_powercycle_error(mut self, code: ErrorCode, msg: &str) -> Self {
            self.powercycle_errors
                .get_mut()
                .unwrap()
                .push_back(RmsError::new(code, msg));
            self
        }

        fn with_power_state_sequence(mut self, states: impl Into<VecDeque<PowerState>>) -> Self {
            *self.power_states.get_mut().unwrap() = states.into();
            self
        }

        fn with_power_state_default(mut self, state: PowerState) -> Self {
            self.power_state_default = state;
            self
        }
    }

    #[async_trait]
    impl Node for RecoveryMockNode {
        fn id(&self) -> &str {
            "recovery-mock-node"
        }
        fn rack_id(&self) -> &str {
            "recovery-mock-rack"
        }
        fn node_type(&self) -> DomainNodeType {
            self.node_type
        }
        fn get_info(&self) -> HashMap<String, String> {
            HashMap::new()
        }
        fn supports_bmc_aux_powercycle(&self) -> bool {
            self.node_type.kind() == NodeKind::Switch
        }

        async fn bmc_aux_powercycle(&self) -> Result<()> {
            self.powercycle_calls.fetch_add(1, Ordering::SeqCst);
            match self.powercycle_errors.lock().unwrap().pop_front() {
                Some(e) => Err(e),
                None => Ok(()),
            }
        }

        async fn get_power_state(&self) -> Result<PowerState> {
            Ok(self
                .power_states
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(self.power_state_default))
        }
    }

    // ── is_unreachable_err ───────────────────────────────────────────────────

    #[test]
    fn is_unreachable_err_true() {
        for code in [
            ErrorCode::ConnectionRefused,
            ErrorCode::Timeout,
            ErrorCode::Unavailable,
        ] {
            let e = RmsError::new(code, "simulated failure");
            assert!(is_unreachable_err(&e), "should be true for {code:?}");
        }
    }

    #[test]
    fn is_unreachable_err_false() {
        for code in [
            ErrorCode::Internal,
            ErrorCode::NotFound,
            ErrorCode::DnsResolutionFailed,
            ErrorCode::InvalidArgument,
            ErrorCode::FailedPrecondition,
        ] {
            let e = RmsError::new(code, "simulated failure");
            assert!(!is_unreachable_err(&e), "should be false for {code:?}");
        }
    }

    // ── attempt_bmc_aux_powercycle_recovery ──────────────────────────────────

    #[tokio::test]
    async fn attempt_recovery_succeeds_when_powercycle_ok_and_power_on_immediately() {
        let node = RecoveryMockNode::switch();
        attempt_bmc_aux_powercycle_recovery(&node).await.unwrap();
        assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn attempt_recovery_polls_until_power_on() {
        let node = RecoveryMockNode::switch().with_power_state_sequence([
            PowerState::Unknown,
            PowerState::Off,
            PowerState::On,
        ]);
        attempt_bmc_aux_powercycle_recovery(&node).await.unwrap();
        assert_eq!(node.powercycle_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn attempt_recovery_propagates_powercycle_error() {
        let node = RecoveryMockNode::switch()
            .with_powercycle_error(ErrorCode::Unavailable, "BMC unreachable");
        let err = attempt_bmc_aux_powercycle_recovery(&node)
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::Unavailable);
        assert!(err.message.contains("BMC unreachable"));
    }

    #[tokio::test]
    async fn attempt_recovery_returns_error_when_power_on_timeout_exceeded() {
        // With cfg(test), NODE_AUX_POWERCYCLE_RECOVERY_TIMEOUT is 200ms.
        // Setting the default power state to Off ensures the loop never finds On
        // and hits the deadline, regardless of how many iterations are run.
        let node = RecoveryMockNode::switch().with_power_state_default(PowerState::Off);
        let err = attempt_bmc_aux_powercycle_recovery(&node)
            .await
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(
            err.message.contains("power-on") || err.message.contains("aux powercycle"),
            "unexpected error: {}",
            err.message
        );
    }
}
