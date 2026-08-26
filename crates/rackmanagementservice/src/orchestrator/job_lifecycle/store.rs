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
use std::time::Duration;

use chrono::{DateTime, Utc};

use super::ids::JobId;
use super::registry::{JobLifecycleState, JobSnapshot};

/// Authoritative job records and the indexes derived from them.
///
/// Every non-terminal job with a non-empty node ID appears in `active_by_node`.
/// Both maps are private so mutations must use methods that preserve that
/// invariant.
#[derive(Default)]
pub(super) struct JobStore {
    /// Authoritative snapshots keyed by their stable job IDs.
    by_id: HashMap<JobId, JobSnapshot>,
    /// Derived rack -> node -> active job IDs index.
    active_by_node: HashMap<String, HashMap<String, HashSet<JobId>>>,
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

    /// Returns any active leaf job ID for the supplied rack and node.
    ///
    /// More than one ID may be indexed when callers use unrestricted leaf
    /// creation for the same node.
    pub(super) fn active_job_for_node(&self, rack_id: &str, node_id: &str) -> Option<JobId> {
        self.active_by_node
            .get(rack_id)
            .and_then(|nodes| nodes.get(node_id))
            .and_then(|jobs| jobs.iter().next())
            .cloned()
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
        parent.updated_at = Utc::now();
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
    /// whether the state changed and carries a snapshot for the first
    /// non-terminal to terminal transition.
    pub(super) fn transition_state(
        &mut self,
        job_id: &JobId,
        transition: impl FnOnce(&JobLifecycleState) -> Option<JobLifecycleState>,
    ) -> (bool, Option<JobSnapshot>) {
        let (index_change, terminal_snapshot) = {
            let Some(record) = self.by_id.get_mut(job_id) else {
                return (false, None);
            };
            let was_indexed = should_index(record);
            let was_terminal = record.state.is_terminal();
            let Some(next_state) = transition(&record.state) else {
                return (false, None);
            };

            record.state = next_state;
            record.updated_at = Utc::now();

            let is_indexed = should_index(record);
            let index_change = (was_indexed != is_indexed).then(|| {
                (
                    record.rack_id.clone(),
                    record.node_id.clone(),
                    record.job_id.clone(),
                    is_indexed,
                )
            });
            let terminal_snapshot =
                (!was_terminal && record.state.is_terminal()).then(|| record.clone());
            (index_change, terminal_snapshot)
        };

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

        (true, terminal_snapshot)
    }

    /// Evicts expired terminal jobs while preserving children of live parents.
    ///
    /// An expired parent that still has retained children remains until a later
    /// pass, after those children have been removed.
    pub(super) fn cleanup_expired(&mut self, now: DateTime<Utc>, ttl: Duration) {
        let protected_children = protected_children(&self.by_id, now, ttl);
        let expired = filter_job_ids(&self.by_id, |jobs, job_id, job| {
            let expired = job.state.is_terminal() && is_older_than(now, job.updated_at, ttl);
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
        }
    }

    /// Asserts that the active-node index exactly matches active leaf records.
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
fn should_index(job: &JobSnapshot) -> bool {
    !job.node_id.is_empty() && !job.state.is_terminal()
}

/// Returns child IDs protected by parents that have not expired.
fn protected_children(
    jobs: &HashMap<JobId, JobSnapshot>,
    now: DateTime<Utc>,
    ttl: Duration,
) -> HashSet<JobId> {
    jobs.values()
        .filter(|job| job.is_parent())
        .filter(|job| {
            let expired = job.state.is_terminal() && is_older_than(now, job.updated_at, ttl);
            !expired
        })
        .flat_map(|job| job.child_job_ids.iter().cloned())
        .collect()
}

/// Returns `true` when `timestamp` is at least `ttl` in the past.
///
/// A non-positive elapsed interval, such as a backwards wall-clock step, is
/// treated as not yet expired.
fn is_older_than(now: DateTime<Utc>, timestamp: DateTime<Utc>, ttl: Duration) -> bool {
    now.signed_duration_since(timestamp)
        .to_std()
        .map(|elapsed| elapsed >= ttl)
        .unwrap_or(false)
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
        let previous_updated_at = store.get(&parent.job_id).unwrap().updated_at;
        let mut child = leaf("child");
        child.parent_job_id = Some(parent.job_id.clone());
        let child = store.insert(child);

        assert!(store.attach_child(&child));

        let parent = store.get(&parent.job_id).unwrap();
        assert_eq!(parent.child_job_ids, vec![child.job_id]);
        assert!(parent.updated_at > previous_updated_at);
    }

    #[test]
    fn terminal_transition_removes_active_index() {
        let mut store = JobStore::default();
        let job = store.insert(leaf("queued"));

        let (changed, terminal_snapshot) = store.transition_state(&job.job_id, |_| {
            Some(JobLifecycleState::Completed {
                description: "complete".to_string(),
                result_json: String::new(),
            })
        });

        assert!(changed);
        assert_eq!(terminal_snapshot.unwrap().job_id, job.job_id);
        assert_eq!(store.by_id.len(), 1);
        assert!(store.active_by_node.is_empty());
        store.assert_consistent();
    }
}
