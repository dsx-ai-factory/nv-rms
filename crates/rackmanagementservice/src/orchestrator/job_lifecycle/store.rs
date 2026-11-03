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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use super::ids::JobId;
use super::registry::{JobLifecycleState, JobScope, JobSnapshot, LifecycleTransition};

/// Authoritative job records and the indexes derived from them.
///
/// Every non-terminal job with a non-empty node ID appears in `active_by_node`.
/// Job aliases remain reserved until a terminal transition; direct-mutation
/// aliases remain reserved until their guard is dropped. All indexes are private
/// and mutate under the registry's write lock.
#[derive(Default)]
pub(super) struct JobStore {
    /// Authoritative snapshots keyed by their stable job IDs.
    by_id: HashMap<JobId, JobSnapshot>,

    /// Derived rack -> node -> active job IDs index.
    active_by_node: HashMap<String, HashMap<String, HashSet<JobId>>>,

    /// Forward alias index shared by tracked jobs and direct-mutation guards.
    active_by_scope: HashMap<JobScope, JobId>,

    /// Reverse index used to release every alias when its owner finishes.
    scopes_by_owner: HashMap<JobId, HashSet<JobScope>>,
}

impl JobStore {
    /// Inserts a job with a vacant ID and keeps the active-node index synchronized.
    ///
    /// A colliding generated ID is replaced with a newly generated ID until
    /// `by_id` has a vacant entry. The returned snapshot contains the final ID.
    /// Non-terminal jobs with a node ID are indexed. Jobs without a node ID and
    /// terminal jobs are retained only in `by_id`.
    pub(super) fn insert(&mut self, record: JobSnapshot) -> JobSnapshot {
        self.insert_with_id_generator(record, JobId::new)
    }

    /// Returns the number of authoritative job records.
    pub(super) fn tracked_job_count(&self) -> usize {
        self.by_id.len()
    }

    /// Returns a borrowed snapshot for a known job.
    pub(super) fn get(&self, job_id: &JobId) -> Option<&JobSnapshot> {
        self.by_id.get(job_id)
    }

    /// Iterates over every authoritative job snapshot.
    pub(super) fn snapshots(&self) -> impl Iterator<Item = &JobSnapshot> {
        self.by_id.values()
    }

    /// Returns any indexed active job ID for the supplied rack and node.
    ///
    /// The index is not restricted to leaves: any non-terminal job carrying
    /// this node is a candidate, including a top-level job that has since
    /// become a parent. More than one ID may be indexed when callers use
    /// unrestricted creation for the same node.
    pub(super) fn active_job_for_node(&self, rack_id: &str, node_id: &str) -> Option<JobId> {
        self.active_by_node
            .get(rack_id)
            .and_then(|nodes| nodes.get(node_id))
            .and_then(|jobs| jobs.iter().next())
            .cloned()
    }

    /// Checks every alias before any reservation is inserted.
    pub(super) fn conflicting_scope(&self, scopes: &[JobScope]) -> Option<(JobScope, JobId)> {
        self.conflicting_scope_except(scopes, None)
    }

    /// Finds overlap while allowing the named worker to reuse its own aliases.
    /// Logical aliases also check jobs created without an explicit scope list.
    pub(super) fn conflicting_scope_except(
        &self,
        scopes: &[JobScope],
        owner_id: Option<&JobId>,
    ) -> Option<(JobScope, JobId)> {
        scopes.iter().find_map(|scope| {
            let active = self
                .active_by_scope
                .get(scope)
                .filter(|active| Some(*active) != owner_id)
                .cloned()
                .or_else(|| {
                    if let JobScope::LogicalNode { rack_id, node_id } = scope {
                        self.active_by_node
                            .get(rack_id)
                            .and_then(|nodes| nodes.get(node_id))
                            .and_then(|jobs| jobs.iter().find(|active| Some(*active) != owner_id))
                            .cloned()
                    } else {
                        None
                    }
                });

            active.map(|owner| (scope.clone(), owner))
        })
    }

    /// Includes direct-mutation owners that have no retained job record.
    pub(super) fn has_scope_owner(&self, owner_id: &JobId) -> bool {
        self.scopes_by_owner.contains_key(owner_id)
    }

    /// Installs all validated aliases while the registry holds its write lock.
    pub(super) fn reserve_scopes(&mut self, owner_id: JobId, scopes: Vec<JobScope>) {
        let scopes: HashSet<_> = scopes.into_iter().collect();

        if scopes.is_empty() {
            return;
        }

        for scope in &scopes {
            self.active_by_scope.insert(scope.clone(), owner_id.clone());
        }

        self.scopes_by_owner
            .entry(owner_id)
            .or_default()
            .extend(scopes);
    }

    /// Removes only the aliases owned by the completed mutation or job.
    pub(super) fn release_scopes(&mut self, owner_id: &JobId) {
        let Some(scopes) = self.scopes_by_owner.remove(owner_id) else {
            return;
        };

        for scope in scopes {
            self.active_by_scope.remove(&scope);
        }
    }

    /// Returns whether a top-level job can accept a newly constructed child.
    pub(super) fn can_accept_child(&self, parent_job_id: &JobId) -> bool {
        let Some(parent) = self.by_id.get(parent_job_id) else {
            return false;
        };
        !parent.state.is_terminal() && parent.parent_job_id.is_none()
    }

    /// Appends an already-inserted child snapshot's ID to its parent.
    pub(super) fn attach_child(&mut self, child: &JobSnapshot) -> bool {
        let Some(parent_job_id) = child.parent_job_id.as_ref() else {
            return false;
        };
        let Some(parent) = self.by_id.get_mut(parent_job_id) else {
            return false;
        };
        if parent.state.is_terminal() || parent.parent_job_id.is_some() {
            return false;
        }
        parent.child_job_ids.push(child.job_id.clone());
        parent.touch();
        true
    }

    /// Removes an unstarted leaf job and its active-node index entry.
    ///
    /// Running, terminal, parent, and parent-linked jobs remain tracked.
    pub(super) fn remove_queued_leaf(&mut self, job_id: &JobId) -> bool {
        let Some(record) = self.by_id.get(job_id) else {
            return false;
        };

        if record.is_parent()
            || record.parent_job_id.is_some()
            || !matches!(record.state, JobLifecycleState::Queued { .. })
        {
            return false;
        }

        self.remove(job_id).is_some()
    }

    /// Applies a lifecycle-state transition and reconciles active-node indexing.
    ///
    /// The transition returns `Some` with the replacement state when it should
    /// be applied, or `None` to leave the job unchanged. The result reports
    /// whether the state changed and classifies the change as a
    /// [`LifecycleTransition`]: the first `Queued -> Running` move
    /// (`Started`), a later `Running -> Running` description update
    /// (`Progress`), or the first non-terminal to terminal move (`Terminal`).
    pub(super) fn transition_state(
        &mut self,
        job_id: &JobId,
        transition: impl FnOnce(&JobLifecycleState) -> Option<JobLifecycleState>,
    ) -> (bool, Option<LifecycleTransition>) {
        let (index_change, event) = {
            let Some(record) = self.by_id.get_mut(job_id) else {
                return (false, None);
            };
            let was_indexed = should_index(record);
            let was_queued = matches!(record.state, JobLifecycleState::Queued { .. });
            let was_running = matches!(record.state, JobLifecycleState::Running { .. });
            let was_terminal = record.state.is_terminal();
            let Some(next_state) = transition(&record.state) else {
                return (false, None);
            };

            record.state = next_state;
            record.touch();

            let is_indexed = should_index(record);
            let index_change = (was_indexed != is_indexed).then(|| {
                (
                    record.rack_id.clone(),
                    record.node_id.clone(),
                    record.job_id.clone(),
                    is_indexed,
                )
            });
            let is_running = matches!(record.state, JobLifecycleState::Running { .. });
            let event = if was_queued && is_running {
                Some(LifecycleTransition::Started(record.clone()))
            } else if was_running && is_running {
                Some(LifecycleTransition::Progress(record.clone()))
            } else if !was_terminal && record.state.is_terminal() {
                Some(LifecycleTransition::Terminal(record.clone()))
            } else {
                None
            };
            (index_change, event)
        };

        if matches!(event, Some(LifecycleTransition::Terminal(_))) {
            self.release_scopes(job_id);
        }

        if let Some((rack_id, node_id, indexed_job_id, should_be_indexed)) = index_change {
            if should_be_indexed {
                self.active_by_node
                    .entry(rack_id)
                    .or_default()
                    .entry(node_id)
                    .or_default()
                    .insert(indexed_job_id);
            } else {
                self.remove_active_job(&rack_id, &node_id, &indexed_job_id);
            }
        }

        (true, event)
    }

    /// Evicts expired terminal jobs while preserving children of live parents.
    ///
    /// An expired parent that still has retained children remains until a later
    /// pass, after those children have been removed.
    pub(super) fn cleanup_expired(&mut self, now: Instant, ttl: Duration) {
        for job in self.by_id.values_mut() {
            if job.state.is_terminal() {
                warn_if_monotonic_timestamp_in_future(job, now);
            }
        }
        let protected_children = protected_children(&self.by_id, now, ttl);
        let expired = filter_job_ids(&self.by_id, |jobs, job_id, job| {
            let expired = job.state.is_terminal() && is_older_than(now, job.updated_at_mono, ttl);
            let removable = !job.is_parent()
                || !job
                    .child_job_ids
                    .iter()
                    .any(|child_job_id| jobs.contains_key(child_job_id));
            expired && !protected_children.contains(job_id) && removable
        });

        for job_id in expired {
            self.remove(&job_id);
        }
    }

    /// Ages a job's timestamps for timestamp-sensitive tests.
    #[cfg(test)]
    pub(super) fn age_job_for_test(&mut self, job_id: &JobId, age: Duration) {
        let delta = chrono::Duration::from_std(age).expect("test age fits in chrono::Duration");
        if let Some(job) = self.by_id.get_mut(job_id) {
            job.created_at -= delta;
            job.updated_at -= delta;
            job.updated_at_mono = job
                .updated_at_mono
                .checked_sub(age)
                .unwrap_or(job.updated_at_mono);
        }
    }

    /// Asserts that the active-node index exactly matches the records
    /// [`should_index`] accepts.
    #[cfg(test)]
    pub(super) fn assert_consistent(&self) {
        let mut expected = HashMap::<String, HashMap<String, HashSet<JobId>>>::new();
        for job in self.by_id.values().filter(|job| should_index(job)) {
            expected
                .entry(job.rack_id.clone())
                .or_default()
                .entry(job.node_id.clone())
                .or_default()
                .insert(job.job_id.clone());
        }

        assert_eq!(self.active_by_node, expected);

        let expected_scopes: HashMap<_, _> = self
            .scopes_by_owner
            .iter()
            .flat_map(|(owner_id, scopes)| {
                if let Some(job) = self.by_id.get(owner_id) {
                    assert!(!job.state.is_terminal());
                }

                scopes.iter().map(|scope| (scope.clone(), owner_id.clone()))
            })
            .collect();

        assert_eq!(self.active_by_scope, expected_scopes);
    }

    /// Inserts a job, using `next_job_id` to retry collisions.
    ///
    /// Keeping the generator explicit here lets tests force repeated collisions
    /// and verify that no existing record is replaced.
    fn insert_with_id_generator(
        &mut self,
        mut record: JobSnapshot,
        mut next_job_id: impl FnMut() -> JobId,
    ) -> JobSnapshot {
        loop {
            if self.has_scope_owner(&record.job_id) {
                record.job_id = next_job_id();
                continue;
            }

            match self.by_id.entry(record.job_id.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(record.clone());
                    break;
                }
                Entry::Occupied(_) => {
                    record.job_id = next_job_id();
                }
            }
        }

        if should_index(&record) {
            self.active_by_node
                .entry(record.rack_id.clone())
                .or_default()
                .entry(record.node_id.clone())
                .or_default()
                .insert(record.job_id.clone());
        }

        record
    }

    /// Removes a job and its exact active-node index entry, if present.
    ///
    /// Returns the removed snapshot, or `None` when the job was not tracked.
    fn remove(&mut self, job_id: &JobId) -> Option<JobSnapshot> {
        let record = self.by_id.remove(job_id)?;
        self.remove_active_job(&record.rack_id, &record.node_id, &record.job_id);
        self.release_scopes(job_id);
        Some(record)
    }

    /// Removes one active job ID and prunes empty node and rack buckets.
    fn remove_active_job(&mut self, rack_id: &str, node_id: &str, job_id: &JobId) {
        let remove_rack = {
            let Some(nodes) = self.active_by_node.get_mut(rack_id) else {
                return;
            };
            let Some(jobs) = nodes.get_mut(node_id) else {
                return;
            };

            jobs.remove(job_id);
            if !jobs.is_empty() {
                return;
            }

            nodes.remove(node_id);
            nodes.is_empty()
        };

        if remove_rack {
            self.active_by_node.remove(rack_id);
        }
    }
}

/// Returns `true` when a job belongs in the active-node index.
///
/// Membership turns only on carrying a node and not being terminal. Being a
/// leaf is deliberately not part of it: a top-level job that later gains
/// children stays indexed against its own node, so it keeps blocking node-idle
/// admission for as long as it is live. Parent-only records built with
/// `JobSpec::new_parent` carry an empty `node_id` and so are never indexed.
fn should_index(job: &JobSnapshot) -> bool {
    !job.node_id.is_empty() && !job.state.is_terminal()
}

/// Returns child IDs protected by parents that have not expired.
fn protected_children(
    jobs: &HashMap<JobId, JobSnapshot>,
    now: Instant,
    ttl: Duration,
) -> HashSet<JobId> {
    jobs.values()
        .filter(|job| job.is_parent())
        .filter(|job| {
            let expired = job.state.is_terminal() && is_older_than(now, job.updated_at_mono, ttl);
            !expired
        })
        .flat_map(|job| job.child_job_ids.iter().cloned())
        .collect()
}

/// Returns `true` when `timestamp` is at least `ttl` in the past on the
/// monotonic clock.
///
/// [`Instant::saturating_duration_since`] treats a `now` that is earlier than
/// `timestamp` as zero elapsed time, so eviction never panics and never fires
/// early if a test (or a clock quirk) inverts the two instants.
fn is_older_than(now: Instant, timestamp: Instant, ttl: Duration) -> bool {
    now.saturating_duration_since(timestamp) >= ttl
}

fn warn_if_monotonic_timestamp_in_future(job: &mut JobSnapshot, now: Instant) {
    if now >= job.updated_at_mono || job.mono_inversion_warned {
        return;
    }
    job.mono_inversion_warned = true;
    tracing::warn!(
        job_id = %job.job_id,
        "monotonic TTL timestamp is ahead of Instant::now(); treating elapsed as zero"
    );
}

/// Collects job IDs whose snapshots satisfy `predicate`.
///
/// The predicate receives the authoritative map so relational checks can
/// inspect other jobs without capturing a separate map reference.
fn filter_job_ids(
    jobs: &HashMap<JobId, JobSnapshot>,
    predicate: impl Fn(&HashMap<JobId, JobSnapshot>, &JobId, &JobSnapshot) -> bool,
) -> Vec<JobId> {
    jobs.iter()
        .filter(|(job_id, job)| predicate(jobs, job_id, job))
        .map(|(job_id, _)| job_id.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn leaf(description: &str) -> JobSnapshot {
        let now = Utc::now();
        JobSnapshot {
            job_id: JobId::new(),
            span: tracing::Span::none(),
            state: JobLifecycleState::Queued {
                description: description.to_string(),
            },
            rack_id: "rack".to_string(),
            node_id: "node".to_string(),
            created_at: now,
            updated_at: now,
            updated_at_mono: Instant::now(),
            mono_inversion_warned: false,
            child_job_ids: Vec::new(),
            parent_job_id: None,
            cancellation_token: CancellationToken::new(),
            workflow_type: None,
        }
    }

    #[test]
    fn insert_retries_colliding_id_without_replacing_existing_job() {
        let mut store = JobStore::default();
        let first = store.insert(leaf("first"));
        let colliding_id = first.job_id.clone();
        let mut colliding = leaf("second");
        colliding.job_id = colliding_id.clone();
        let vacant_id = JobId::from_existing("known-vacant-id");
        let mut generated_ids = [colliding_id.clone(), vacant_id.clone()].into_iter();

        let second = store.insert_with_id_generator(colliding, || {
            generated_ids.next().expect("test ID sequence exhausted")
        });

        assert_eq!(second.job_id, vacant_id);
        assert!(generated_ids.next().is_none());
        assert_eq!(store.by_id.len(), 2);
        assert_eq!(
            store.by_id.get(&colliding_id).unwrap().state.description(),
            "first"
        );
        assert_eq!(
            store.by_id.get(&second.job_id).unwrap().state.description(),
            "second"
        );
        let active_jobs = &store.active_by_node["rack"]["node"];
        assert_eq!(active_jobs.len(), 2);
        assert!(active_jobs.contains(&colliding_id));
        assert!(active_jobs.contains(&vacant_id));
        store.assert_consistent();
    }

    #[test]
    fn removing_queued_leaf_prunes_active_index() {
        let mut store = JobStore::default();
        let job = store.insert(leaf("queued"));

        assert!(store.remove_queued_leaf(&job.job_id));

        assert!(store.by_id.is_empty());
        assert!(store.active_by_node.is_empty());
        store.assert_consistent();
    }

    #[test]
    fn attaching_child_refreshes_parent_updated_at() {
        let mut store = JobStore::default();
        let parent = store.insert(leaf("parent"));
        store.age_job_for_test(&parent.job_id, Duration::from_secs(1));
        let previous = store.get(&parent.job_id).unwrap();
        let previous_updated_at = previous.updated_at;
        let previous_updated_at_mono = previous.updated_at_mono;
        let mut child = leaf("child");
        child.parent_job_id = Some(parent.job_id.clone());
        let child = store.insert(child);

        assert!(store.attach_child(&child));

        let parent = store.get(&parent.job_id).unwrap();
        assert_eq!(parent.child_job_ids, vec![child.job_id]);
        assert!(parent.updated_at > previous_updated_at);
        assert!(parent.updated_at_mono > previous_updated_at_mono);
    }

    #[test]
    fn terminal_transition_removes_active_index() {
        let mut store = JobStore::default();
        let job = store.insert(leaf("queued"));

        let (changed, event) = store.transition_state(&job.job_id, |_| {
            Some(JobLifecycleState::Completed {
                description: "complete".to_string(),
                result_json: String::new(),
            })
        });

        assert!(changed);
        let Some(LifecycleTransition::Terminal(terminal_snapshot)) = event else {
            panic!("expected a Terminal transition event");
        };
        assert_eq!(terminal_snapshot.job_id, job.job_id);
        assert_eq!(store.by_id.len(), 1);
        assert!(store.active_by_node.is_empty());
        store.assert_consistent();
    }

    #[test]
    fn queued_to_running_transition_reports_started() {
        let mut store = JobStore::default();
        let job = store.insert(leaf("queued"));

        let (changed, event) = store.transition_state(&job.job_id, |_| {
            Some(JobLifecycleState::Running {
                description: "running".to_string(),
            })
        });

        assert!(changed);
        let Some(LifecycleTransition::Started(started_snapshot)) = event else {
            panic!("expected a Started transition event");
        };
        assert_eq!(started_snapshot.job_id, job.job_id);
    }

    #[test]
    fn running_to_running_transition_reports_progress() {
        let mut store = JobStore::default();
        let job = store.insert(leaf("queued"));
        store.transition_state(&job.job_id, |_| {
            Some(JobLifecycleState::Running {
                description: "running".to_string(),
            })
        });

        let (changed, event) = store.transition_state(&job.job_id, |_| {
            Some(JobLifecycleState::Running {
                description: "still running".to_string(),
            })
        });

        assert!(changed);
        let Some(LifecycleTransition::Progress(progress_snapshot)) = event else {
            panic!("expected a Progress transition event");
        };
        assert_eq!(progress_snapshot.job_id, job.job_id);
    }

    fn complete_leaf(store: &mut JobStore, job_id: &JobId) {
        store.transition_state(job_id, |_| {
            Some(JobLifecycleState::Completed {
                description: "complete".to_string(),
                result_json: String::new(),
            })
        });
    }

    #[test]
    fn ttl_ignores_stale_wall_clock_updated_at() {
        let mut store = JobStore::default();
        let job = store.insert(leaf("queued"));
        complete_leaf(&mut store, &job.job_id);
        let ttl = Duration::from_secs(60);

        store.by_id.get_mut(&job.job_id).unwrap().updated_at -= chrono::Duration::hours(48);

        store.cleanup_expired(Instant::now(), ttl);
        assert!(
            store.get(&job.job_id).is_some(),
            "a wall-clock jump into the past must not evict a still-young job"
        );
    }

    #[test]
    fn ttl_evicts_from_monotonic_age_even_when_wall_clock_looks_fresh() {
        let mut store = JobStore::default();
        let job = store.insert(leaf("queued"));
        complete_leaf(&mut store, &job.job_id);
        let ttl = Duration::from_secs(60);

        store.age_job_for_test(&job.job_id, ttl + Duration::from_secs(1));
        store.by_id.get_mut(&job.job_id).unwrap().updated_at = Utc::now();

        store.cleanup_expired(Instant::now(), ttl);
        assert!(
            store.get(&job.job_id).is_none(),
            "a wall-clock jump into the future must not retain an aged terminal job"
        );
    }

    #[test]
    fn ttl_treats_future_monotonic_timestamp_as_not_expired() {
        let mut store = JobStore::default();
        let job = store.insert(leaf("queued"));
        complete_leaf(&mut store, &job.job_id);
        let ttl = Duration::from_secs(60);

        store.by_id.get_mut(&job.job_id).unwrap().updated_at_mono =
            Instant::now() + Duration::from_secs(3600);

        store.cleanup_expired(Instant::now(), ttl);
        assert!(store.get(&job.job_id).is_some());
        assert!(
            store
                .get(&job.job_id)
                .expect("job retained")
                .mono_inversion_warned
        );

        store.cleanup_expired(Instant::now(), ttl);
        assert!(
            store
                .get(&job.job_id)
                .expect("job retained")
                .mono_inversion_warned,
            "a second sweep must not clear the one-shot warn flag"
        );
    }

    #[test]
    fn ttl_does_not_warn_on_live_jobs_with_future_monotonic_timestamp() {
        let mut store = JobStore::default();
        let job = store.insert(leaf("queued"));
        let ttl = Duration::from_secs(60);

        store.by_id.get_mut(&job.job_id).unwrap().updated_at_mono =
            Instant::now() + Duration::from_secs(3600);

        store.cleanup_expired(Instant::now(), ttl);
        assert!(
            !store
                .get(&job.job_id)
                .expect("live job")
                .mono_inversion_warned
        );
    }
}
