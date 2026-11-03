/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Process-local ownership and verified controller activation evidence.
//!
//! Coordinators can share an `Arc<FabricOperationRegistry>`, constructed with
//! `FabricOperationRegistry::default()`. Restart loses every operation and proof;
//! a missing proof requires a verified activation before claiming convergence.
//! RPC integration is not enabled by this module.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Mutex, MutexGuard};

use crate::utilities::error::{Result, RmsError};

/// Active and unresolved operations share capacity and are never evicted.
const MAX_OPERATIONS: usize = 256;

/// The oldest successful proof becomes unknown when a new proof exceeds capacity.
const MAX_ACTIVATION_PROOFS: usize = 1024;

/// Physical controller identity independent of request endpoint aliases.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ControllerIdentity {
    /// Independently observed chassis identity.
    pub(crate) chassis_serial_number: String,

    /// Independently observed switch tray position.
    pub(crate) tray_index: u32,
}

/// Credential-free operation scope and deployment rule identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FabricOperationIntent {
    /// Caller-canonicalized identities and logical, host, BMC, and SSH aliases.
    /// Keys use exact equality; the registry only sorts and deduplicates them.
    pub(crate) scope_keys: Vec<String>,

    /// Digest identifying the selected physical-layout artifact.
    pub(crate) rule_id: String,
}

impl FabricOperationIntent {
    fn normalize(&mut self) -> Result<()> {
        if self.scope_keys.is_empty()
            || self.scope_keys.iter().any(|key| key.trim().is_empty())
            || self.rule_id.trim().is_empty()
        {
            return Err(RmsError::invalid_argument(
                "fabric operation requires scope keys and a rule identity",
            ));
        }

        self.scope_keys.sort();
        self.scope_keys.dedup();

        Ok(())
    }
}

/// Current job owner and controller activations that remain unverified.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FabricOperation {
    /// Job identity fencing every operation mutation.
    pub(crate) job_id: String,

    /// Accepted scope and deployment rule identity for the current owner.
    pub(crate) intent: FabricOperationIntent,

    /// Physical controllers requiring verified shutdown or activation.
    pub(crate) pending_controllers: BTreeSet<ControllerIdentity>,
}

/// Successful activation evidence, not a completed-job result or request history.
struct ActivationProof {
    controller: ControllerIdentity,
    digest: String,
    scope_keys: Vec<String>,
}

#[derive(Default)]
struct RegistryState {
    operations: BTreeMap<String, FabricOperation>,
    proofs: VecDeque<ActivationProof>,
}

/// Tracks unresolved controller activation and verified activation proofs.
///
/// Callers hold all physical job scopes and prove the previous job terminal before
/// transferring ownership. Job admission reserves devices while workers run;
/// coordinators release failed ownership after draining work and attempting cleanup.
/// The mutex fences registry writes, not remote device I/O.
#[derive(Default)]
pub(crate) struct FabricOperationRegistry {
    state: Mutex<RegistryState>,
}

impl FabricOperationRegistry {
    /// Every registry operation fails closed when a prior mutation poisoned the lock.
    fn lock(&self) -> Result<MutexGuard<'_, RegistryState>> {
        self.state
            .lock()
            .map_err(|_| RmsError::internal("fabric operation registry lock poisoned"))
    }

    /// Returns active or unresolved operations sharing any identity or alias.
    /// Results are ordered by job ID; an empty scope returns no records.
    pub(crate) fn find_overlapping(&self, scope_keys: &[String]) -> Result<Vec<FabricOperation>> {
        Ok(self
            .lock()?
            .operations
            .values()
            .filter(|operation| {
                operation
                    .intent
                    .scope_keys
                    .iter()
                    .any(|key| scope_keys.contains(key))
            })
            .cloned()
            .collect())
    }

    /// Atomically admits disjoint scope or transfers an explicitly named terminal job.
    /// Transfer requires identical scope and rule, retains pending controllers, and
    /// fences the old job ID. The caller proves termination while holding all scopes.
    /// Conflicting ownership and capacity reject admission without changing evidence.
    pub(crate) fn admit(
        &self,
        mut intent: FabricOperationIntent,
        job_id: &str,
        previous_job_id: Option<&str>,
    ) -> Result<FabricOperation> {
        intent.normalize()?;

        if job_id.trim().is_empty() {
            return Err(RmsError::invalid_argument(
                "fabric operation requires a job ID",
            ));
        }

        let mut state = self.lock()?;

        if state.operations.contains_key(job_id) {
            return Err(RmsError::already_exists(
                "fabric job already owns an operation",
            ));
        }

        let mut overlapping = state.operations.values().filter(|operation| {
            operation
                .intent
                .scope_keys
                .iter()
                .any(|key| intent.scope_keys.binary_search(key).is_ok())
        });

        let previous = overlapping.next();

        if overlapping.next().is_some() {
            return Err(RmsError::already_exists(
                "multiple fabric operations own overlapping scope",
            ));
        }

        let pending_controllers = match (previous, previous_job_id) {
            (None, None) => {
                if state.operations.len() >= MAX_OPERATIONS {
                    return Err(RmsError::failed_precondition(
                        "fabric operation registry capacity reached",
                    ));
                }

                BTreeSet::new()
            }
            (Some(operation), Some(previous_job_id)) => {
                if operation.job_id != previous_job_id
                    || operation.intent.scope_keys != intent.scope_keys
                    || operation.intent.rule_id != intent.rule_id
                {
                    return Err(RmsError::failed_precondition(
                        "fabric operation owner, scope, or rule changed",
                    ));
                }

                operation.pending_controllers.clone()
            }
            (Some(_), None) => {
                return Err(RmsError::already_exists(
                    "fabric operation owns overlapping scope",
                ));
            }
            (None, Some(_)) => {
                return Err(RmsError::failed_precondition(
                    "previous fabric operation is absent",
                ));
            }
        };

        let operation = FabricOperation {
            job_id: job_id.to_owned(),
            intent,
            pending_controllers,
        };

        if let Some(previous_job_id) = previous_job_id {
            state.operations.remove(previous_job_id);
        }

        state
            .operations
            .insert(job_id.to_owned(), operation.clone());

        Ok(operation)
    }

    /// Adds pending activation and removes each controller's proof before device writes.
    /// A controller pending under another owner rejects the entire batch unchanged.
    /// Repeating a pending mark for the same owner is allowed.
    pub(crate) fn mark_pending(
        &self,
        job_id: &str,
        controllers: &[ControllerIdentity],
    ) -> Result<()> {
        let mut state = self.lock()?;

        if !state.operations.contains_key(job_id) {
            return Err(RmsError::failed_precondition(
                "fabric operation owner changed",
            ));
        }

        if state.operations.values().any(|operation| {
            operation.job_id != job_id
                && controllers
                    .iter()
                    .any(|controller| operation.pending_controllers.contains(controller))
        }) {
            return Err(RmsError::already_exists(
                "controller activation is pending under another fabric operation",
            ));
        }

        let operation = state
            .operations
            .get_mut(job_id)
            .ok_or_else(|| RmsError::failed_precondition("fabric operation owner changed"))?;

        operation
            .pending_controllers
            .extend(controllers.iter().cloned());

        state
            .proofs
            .retain(|proof| !controllers.contains(&proof.controller));

        Ok(())
    }

    /// Clears pending activation only after the worker verifies the controller disabled.
    /// Succeeds without changes if the current owner has no pending entry for it.
    pub(crate) fn clear_inactive(
        &self,
        job_id: &str,
        controller: &ControllerIdentity,
    ) -> Result<()> {
        let mut state = self.lock()?;

        let operation = state
            .operations
            .get_mut(job_id)
            .ok_or_else(|| RmsError::failed_precondition("fabric operation owner changed"))?;

        operation.pending_controllers.remove(controller);

        Ok(())
    }

    /// Returns whether the process retains a successful proof for the exact desired digest.
    pub(crate) fn has_activation_proof(
        &self,
        controller: &ControllerIdentity,
        digest: &str,
    ) -> Result<bool> {
        Ok(self
            .lock()?
            .proofs
            .iter()
            .any(|proof| proof.controller == *controller && proof.digest == digest))
    }

    /// Publishes verified activation and clears matching pending evidence atomically.
    /// The controller must be pending and its aliases must overlap the owner's scope.
    /// The coordinator supplies every alias for the fabric's member switches.
    /// Proofs retain only those aliases so unrelated switch mutations preserve them.
    /// Evicting an older proof makes its desired state unknown.
    pub(crate) fn record_activation(
        &self,
        job_id: &str,
        controller: ControllerIdentity,
        digest: String,
        mut scope_keys: Vec<String>,
    ) -> Result<()> {
        if digest.trim().is_empty()
            || scope_keys.is_empty()
            || scope_keys.iter().any(|key| key.trim().is_empty())
        {
            return Err(RmsError::invalid_argument(
                "activation proof requires a digest and scope aliases",
            ));
        }

        scope_keys.sort();
        scope_keys.dedup();

        let mut state = self.lock()?;

        let operation = state
            .operations
            .get_mut(job_id)
            .ok_or_else(|| RmsError::failed_precondition("fabric operation owner changed"))?;

        if !scope_keys
            .iter()
            .any(|key| operation.intent.scope_keys.binary_search(key).is_ok())
        {
            return Err(RmsError::failed_precondition(
                "activation proof does not overlap the owned scope",
            ));
        }

        if !operation.pending_controllers.contains(&controller) {
            return Err(RmsError::failed_precondition(
                "controller activation is not pending for the current owner",
            ));
        }

        operation.pending_controllers.remove(&controller);
        state.proofs.retain(|proof| proof.controller != controller);

        state.proofs.push_back(ActivationProof {
            controller,
            digest,
            scope_keys,
        });

        if state.proofs.len() > MAX_ACTIVATION_PROOFS {
            state.proofs.pop_front();
        }

        Ok(())
    }

    /// Releases verified completion; successful activation proofs survive completion.
    pub(crate) fn finish(&self, job_id: &str) -> Result<()> {
        let mut state = self.lock()?;

        let operation = state
            .operations
            .get(job_id)
            .ok_or_else(|| RmsError::failed_precondition("fabric operation owner changed"))?;

        if !operation.pending_controllers.is_empty() {
            return Err(RmsError::failed_precondition(
                "controller activation remains pending",
            ));
        }

        state.operations.remove(job_id);

        Ok(())
    }

    /// Releases a failed operation, leaving pending controllers unverified.
    ///
    /// The caller drains device work and finishes cleanup while still holding job
    /// reservations. Pending controllers lose proof; verified sibling proofs survive.
    pub(crate) fn release_failed(&self, job_id: &str) -> Result<()> {
        let mut state = self.lock()?;

        let operation = state
            .operations
            .remove(job_id)
            .ok_or_else(|| RmsError::failed_precondition("fabric operation owner changed"))?;

        state
            .proofs
            .retain(|proof| !operation.pending_controllers.contains(&proof.controller));

        Ok(())
    }

    /// Rejects active or unresolved overlap, then removes proofs sharing any target alias.
    /// Requires a nonempty list of nonblank, caller-canonicalized scope aliases.
    /// External mutators call this while holding job scopes and before device I/O.
    pub(crate) fn invalidate_and_require_clear(&self, scope_keys: &[String]) -> Result<()> {
        if scope_keys.is_empty() || scope_keys.iter().any(|key| key.trim().is_empty()) {
            return Err(RmsError::invalid_argument(
                "fabric invalidation requires nonempty scope aliases",
            ));
        }

        let mut state = self.lock()?;

        if state.operations.values().any(|operation| {
            operation
                .intent
                .scope_keys
                .iter()
                .any(|key| scope_keys.contains(key))
        }) {
            return Err(RmsError::already_exists(
                "fabric operation owns external mutation scope",
            ));
        }

        state
            .proofs
            .retain(|proof| !proof.scope_keys.iter().any(|key| scope_keys.contains(key)));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utilities::error::ErrorCode;

    fn intent(scope: &str) -> FabricOperationIntent {
        FabricOperationIntent {
            scope_keys: vec![scope.into()],
            rule_id: "physical-rule".into(),
        }
    }

    fn controller(tray_index: u32) -> ControllerIdentity {
        ControllerIdentity {
            chassis_serial_number: "rack-1".into(),
            tray_index,
        }
    }

    #[test]
    fn failed_release_clears_pending_ownership_and_preserves_verified_siblings() -> Result<()> {
        let registry = FabricOperationRegistry::default();
        let accepted = intent("physical:tray:1");

        registry.admit(accepted.clone(), "failed", None)?;
        registry.mark_pending("failed", &[controller(1), controller(2)])?;

        registry.record_activation(
            "failed",
            controller(1),
            "unverified".into(),
            accepted.scope_keys.clone(),
        )?;

        registry.record_activation(
            "failed",
            controller(2),
            "verified".into(),
            accepted.scope_keys.clone(),
        )?;

        registry.mark_pending("failed", &[controller(1)])?;

        assert!(registry.finish("failed").is_err());

        registry.release_failed("failed")?;

        assert!(registry.find_overlapping(&accepted.scope_keys)?.is_empty());
        assert!(!registry.has_activation_proof(&controller(1), "unverified")?);
        assert!(registry.has_activation_proof(&controller(2), "verified")?);

        registry.admit(accepted.clone(), "next", None)?;

        assert!(registry.release_failed("failed").is_err());

        assert_eq!(
            registry.find_overlapping(&accepted.scope_keys)?[0].job_id,
            "next"
        );

        registry.finish("next")?;
        registry.invalidate_and_require_clear(&accepted.scope_keys)?;

        assert!(!registry.has_activation_proof(&controller(2), "verified")?);

        Ok(())
    }

    #[test]
    fn transfer_preserves_pending_and_fences_the_old_job() -> Result<()> {
        let registry = FabricOperationRegistry::default();
        let mut accepted = intent("physical:tray:1");

        accepted
            .scope_keys
            .extend(["endpoint:host".into(), "endpoint:host".into()]);

        registry.admit(accepted.clone(), "old-job", None)?;
        registry.mark_pending("old-job", &[controller(1)])?;

        accepted.scope_keys.reverse();

        let recovered = registry.admit(accepted, "new-job", Some("old-job"))?;

        assert_eq!(
            recovered.pending_controllers,
            BTreeSet::from([controller(1)])
        );

        assert_eq!(
            recovered.intent.scope_keys,
            vec!["endpoint:host", "physical:tray:1"]
        );

        for result in [
            registry.mark_pending("old-job", &[controller(2)]),
            registry.clear_inactive("old-job", &controller(1)),
            registry.record_activation(
                "old-job",
                controller(1),
                "group-digest".into(),
                vec!["endpoint:host".into()],
            ),
            registry.finish("old-job"),
        ] {
            assert_eq!(
                result.expect_err("old job must be fenced").code,
                ErrorCode::FailedPrecondition
            );
        }

        registry.clear_inactive("new-job", &controller(1))?;
        registry.finish("new-job")?;

        assert!(
            registry
                .find_overlapping(&["endpoint:host".into()])?
                .is_empty()
        );

        Ok(())
    }

    #[test]
    fn admission_cannot_steal_active_or_changed_scope_and_rule() -> Result<()> {
        let registry = FabricOperationRegistry::default();

        registry.admit(intent("shared"), "owner", None)?;

        assert_eq!(
            registry
                .admit(intent("shared"), "other", None)
                .expect_err("active overlap must reject")
                .code,
            ErrorCode::AlreadyExists
        );

        for mut replacement in [intent("other-scope"), intent("shared")] {
            if replacement.scope_keys == vec!["shared"] {
                replacement.rule_id = "changed-rule".into();
            }

            registry
                .admit(replacement, "other", Some("owner"))
                .expect_err("scope and rule cannot change during transfer");
        }

        registry
            .admit(intent("shared"), "other", Some("wrong-owner"))
            .expect_err("different owner cannot authorize transfer");

        let mut overlapping = intent("shared");
        overlapping.scope_keys.push("extra".into());

        let error = registry
            .admit(overlapping, "other", Some("owner"))
            .expect_err("overlapping scope cannot change during transfer");

        assert_eq!(error.code, ErrorCode::FailedPrecondition);
        assert_eq!(registry.find_overlapping(&["shared".into()])?.len(), 1);

        assert_eq!(
            registry.find_overlapping(&["shared".into()])?[0].job_id,
            "owner"
        );

        Ok(())
    }

    #[test]
    fn unresolved_capacity_rejects_new_scope_but_allows_transfer() -> Result<()> {
        let registry = FabricOperationRegistry::default();

        for index in 0..MAX_OPERATIONS {
            registry.admit(
                intent(&format!("scope:{index}")),
                &format!("job:{index}"),
                None,
            )?;
        }

        registry.mark_pending("job:0", &[controller(1)])?;

        let error = registry
            .admit(intent("new-scope"), "new-job", None)
            .expect_err("unresolved operations must never be evicted");

        assert_eq!(error.code, ErrorCode::FailedPrecondition);

        let recovered = registry.admit(intent("scope:0"), "recovered", Some("job:0"))?;

        assert_eq!(
            recovered.pending_controllers,
            BTreeSet::from([controller(1)])
        );

        assert_eq!(registry.find_overlapping(&["scope:0".into()])?.len(), 1);

        Ok(())
    }

    #[test]
    fn successful_proof_survives_finish_and_pending_invalidates_exact_controller() -> Result<()> {
        let registry = FabricOperationRegistry::default();
        let identity = controller(1);

        registry.admit(intent("group-scope"), "job", None)?;
        registry.mark_pending("job", std::slice::from_ref(&identity))?;

        registry
            .finish("job")
            .expect_err("pending activation must block finish");

        registry.record_activation(
            "job",
            identity.clone(),
            "group-digest".into(),
            vec!["group-scope".into()],
        )?;

        registry.finish("job")?;

        assert!(registry.has_activation_proof(&identity, "group-digest")?);
        assert!(!registry.has_activation_proof(&identity, "different-digest")?);

        registry.admit(intent("group-scope"), "next-job", None)?;
        registry.mark_pending("next-job", &[controller(2)])?;

        assert!(registry.has_activation_proof(&identity, "group-digest")?);

        registry.mark_pending("next-job", std::slice::from_ref(&identity))?;

        assert!(!registry.has_activation_proof(&identity, "group-digest")?);

        Ok(())
    }

    #[test]
    fn external_alias_invalidation_rejects_overlap_without_losing_proof() -> Result<()> {
        for alias in ["physical", "logical", "host", "bmc", "ssh"] {
            let registry = FabricOperationRegistry::default();

            let aliases: Vec<String> = ["physical", "logical", "host", "bmc", "ssh"]
                .map(String::from)
                .into();

            let mut accepted = intent("physical");

            accepted.scope_keys = aliases.clone();
            accepted
                .scope_keys
                .extend(["sibling".into(), "unassigned".into()]);

            registry.admit(accepted, "job", None)?;
            registry.mark_pending("job", &[controller(1), controller(2)])?;

            registry.record_activation("job", controller(1), "group-digest".into(), aliases)?;

            registry.record_activation(
                "job",
                controller(2),
                "sibling-digest".into(),
                vec!["sibling".into()],
            )?;

            for target in [alias, "sibling", "unassigned"] {
                registry
                    .invalidate_and_require_clear(&[target.into()])
                    .expect_err("active overlap must prevent external mutation");
            }

            assert!(registry.has_activation_proof(&controller(1), "group-digest")?);
            assert!(registry.has_activation_proof(&controller(2), "sibling-digest")?);

            registry.finish("job")?;
            registry.invalidate_and_require_clear(&["unassigned".into()])?;

            assert!(registry.has_activation_proof(&controller(1), "group-digest")?);
            assert!(registry.has_activation_proof(&controller(2), "sibling-digest")?);

            registry.invalidate_and_require_clear(&[alias.into()])?;

            assert!(!registry.has_activation_proof(&controller(1), "group-digest")?);
            assert!(registry.has_activation_proof(&controller(2), "sibling-digest")?);
        }

        Ok(())
    }

    #[test]
    fn oldest_proof_eviction_becomes_unknown_without_evicting_new_proofs() -> Result<()> {
        let registry = FabricOperationRegistry::default();

        registry.admit(intent("group-scope"), "job", None)?;

        for index in 0..=MAX_ACTIVATION_PROOFS {
            registry.mark_pending("job", &[controller(index as u32)])?;

            registry.record_activation(
                "job",
                controller(index as u32),
                "group-digest".into(),
                vec!["group-scope".into()],
            )?;
        }

        registry.finish("job")?;

        assert!(!registry.has_activation_proof(&controller(0), "group-digest")?);
        assert!(registry.has_activation_proof(&controller(1), "group-digest")?);

        assert!(
            registry
                .has_activation_proof(&controller(MAX_ACTIVATION_PROOFS as u32), "group-digest")?
        );

        Ok(())
    }

    #[test]
    fn admission_validates_intent_and_unique_job_identity() -> Result<()> {
        let registry = FabricOperationRegistry::default();

        for scope in ["", " "] {
            assert_eq!(
                registry
                    .admit(intent(scope), "job", None)
                    .expect_err("invalid scope must reject")
                    .code,
                ErrorCode::InvalidArgument
            );
        }

        registry.admit(intent("scope:1"), "job", None)?;

        registry
            .admit(intent("scope:2"), "job", None)
            .expect_err("one job cannot own multiple operations");

        registry.mark_pending("job", &[controller(1)])?;
        registry
            .record_activation(
                "job",
                controller(1),
                "digest".into(),
                vec!["unowned".into()],
            )
            .expect_err("proof aliases must overlap admitted scope");

        assert!(!registry.has_activation_proof(&controller(1), "digest")?);

        Ok(())
    }

    #[test]
    fn conflicting_pending_batch_preserves_operations_and_proofs() -> Result<()> {
        let registry = FabricOperationRegistry::default();

        registry.admit(intent("scope-a"), "owner-a", None)?;
        registry.admit(intent("scope-b"), "owner-b", None)?;
        registry.mark_pending("owner-a", &[controller(1)])?;
        registry.mark_pending("owner-b", &[controller(2)])?;

        registry.record_activation(
            "owner-b",
            controller(2),
            "digest".into(),
            vec!["scope-b".into()],
        )?;

        let error = registry
            .mark_pending("owner-b", &[controller(2), controller(1)])
            .expect_err("another owner has a pending controller");

        assert_eq!(error.code, ErrorCode::AlreadyExists);
        assert!(registry.has_activation_proof(&controller(2), "digest")?);
        registry.finish("owner-b")?;

        // Repeating a pending mark or verified shutdown for the same owner is allowed.
        registry.mark_pending("owner-a", &[controller(1)])?;

        let pending = registry.find_overlapping(&["scope-a".into()])?;

        assert_eq!(
            pending[0].pending_controllers,
            BTreeSet::from([controller(1)])
        );

        assert!(!registry.has_activation_proof(&controller(1), "digest")?);

        registry.clear_inactive("owner-a", &controller(1))?;
        registry.clear_inactive("owner-a", &controller(1))?;
        registry.finish("owner-a")?;

        Ok(())
    }

    #[test]
    fn invalid_invalidation_scope_preserves_activation_proof() -> Result<()> {
        let registry = FabricOperationRegistry::default();

        registry.admit(intent("scope"), "owner", None)?;
        registry.mark_pending("owner", &[controller(1)])?;

        registry.record_activation(
            "owner",
            controller(1),
            "digest".into(),
            vec!["scope".into()],
        )?;

        registry.finish("owner")?;

        for aliases in [vec![], vec!["".into()], vec!["scope".into(), " ".into()]] {
            let error = registry
                .invalidate_and_require_clear(&aliases)
                .expect_err("invalidation requires nonempty aliases");

            assert_eq!(error.code, ErrorCode::InvalidArgument);
            assert!(registry.has_activation_proof(&controller(1), "digest")?);
        }

        Ok(())
    }

    #[test]
    fn proof_publication_requires_pending_identity_and_retains_extra_logical_aliases() -> Result<()>
    {
        let registry = FabricOperationRegistry::default();
        let aliases = vec!["physical:tray:1".into(), "logical:renamed".into()];

        registry.admit(intent("physical:tray:1"), "job", None)?;

        registry
            .record_activation("job", controller(1), "group-digest".into(), aliases.clone())
            .expect_err("publication requires pending ownership");

        registry.mark_pending("job", &[controller(1)])?;
        registry.record_activation("job", controller(1), "group-digest".into(), aliases)?;
        registry.finish("job")?;
        registry.invalidate_and_require_clear(&["logical:renamed".into()])?;

        assert!(!registry.has_activation_proof(&controller(1), "group-digest")?);

        Ok(())
    }

    #[test]
    fn conflicting_concurrent_admission_has_one_owner() -> Result<()> {
        let registry = std::sync::Arc::new(FabricOperationRegistry::default());
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));

        let handles: Vec<_> = ["left", "right"]
            .into_iter()
            .map(|job| {
                let registry = std::sync::Arc::clone(&registry);
                let barrier = std::sync::Arc::clone(&barrier);

                std::thread::spawn(move || {
                    barrier.wait();
                    registry.admit(intent("shared"), job, None)
                })
            })
            .collect();

        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().expect("admission worker must not panic"))
            .collect();

        assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
        assert_eq!(registry.find_overlapping(&["shared".into()])?.len(), 1);

        Ok(())
    }
}
