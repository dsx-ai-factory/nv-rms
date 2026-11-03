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

//! Capacity reclamation for the NVOS entity-certificate and CA-certificate
//! stores.
//!
//! NVOS limits each store to [`NVOS_CERTIFICATE_STORE_LIMIT`] imported
//! entries, and every certificate install imports one entry into each under
//! fresh timestamped IDs. RMS keeps superseded entries until a store has no
//! room for the next import. It then deletes the oldest entries RMS created
//! that no switch service uses, and only as many as the import needs.
//!
//! Deletion requires positive evidence. An entry is deleted only when:
//!
//! - its ID starts with [`RMS_CERTIFICATE_ID_PREFIX`] and uses only ASCII
//!   letters, digits, `-`, and `_`,
//! - it is not material the running install needs,
//! - NVOS reports no service using it, in a shape RMS recognizes, and
//! - every switch service's binding for that store was read and none of
//!   them names it, including services the request does not bind.
//!
//! A binding that cannot be read, or reads back in an unrecognized shape,
//! blocks deletion in its whole store, because RMS cannot tell which entry
//! that service uses.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use chrono::NaiveDateTime;
use serde::Serialize;
use serde_json::Value;

use super::super::SwitchGb200Nvidia;
use super::super::nvue::{extract_job_id, rms_http_status_code};
use super::super::validation::is_valid_identifier;
use super::workflows::{
    NVUE_CA_CERTIFICATE_COLLECTION, NVUE_ENTITY_CERTIFICATE_COLLECTION, NVUE_SYSTEM_API_MTLS_PATH,
    NVUE_SYSTEM_API_PATH, REMOTE_RUN_ID_FORMAT, SwitchMtlsService,
    nvue_cluster_app_manager_sub_endpoint,
};
use crate::transport::http_client::HttpClient;
use crate::utilities::error::{ErrorCode, Result, RmsError};
use nvue_client::action::SimpleAction;
use nvue_client::system::{GNMI_SERVER_ENDPOINT, GNMI_SERVER_MTLS_ENDPOINT};

const NVUE_DELETE_ACTION: &str = "@delete";

/// Entries NVOS accepts in each certificate store.
///
/// NVOS documents the limit as 25 imported entity certificates and 25
/// imported CA certificates. RMS counts every listed entry, including entries
/// RMS did not create, so a store with non-RMS entries reclaims sooner.
pub(crate) const NVOS_CERTIFICATE_STORE_LIMIT: usize = 25;

/// Prefix of every certificate-store ID RMS creates.
pub(crate) const RMS_CERTIFICATE_ID_PREFIX: &str = "rms-";

/// Length of the timestamp suffix [`REMOTE_RUN_ID_FORMAT`] produces.
const IMPORT_TIMESTAMP_LEN: usize = "2025-06-24_12-30-45_123".len();

/// One of the two NVOS certificate stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub(crate) enum CertificateStore {
    /// `/system/security/certificate`: entity certificates and their keys.
    #[serde(rename = "entity-certificate")]
    Entity,

    /// `/system/security/ca-certificate`: CA certificates.
    #[serde(rename = "ca-certificate")]
    Ca,
}

impl CertificateStore {
    pub(crate) const ALL: [Self; 2] = [Self::Entity, Self::Ca];

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Entity => "entity-certificate",
            Self::Ca => "ca-certificate",
        }
    }

    fn collection_endpoint(self) -> &'static str {
        match self {
            Self::Entity => NVUE_ENTITY_CERTIFICATE_COLLECTION,
            Self::Ca => NVUE_CA_CERTIFICATE_COLLECTION,
        }
    }

    fn entry_endpoint(self, id: &str) -> String {
        format!("{}/{id}", self.collection_endpoint())
    }
}

impl fmt::Display for CertificateStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What NVOS reports about an entry's own use, from its `installed` field.
///
/// NVOS reports this only for some consumers, such as the NVUE REST API and
/// the gNMI server, so a [`Self::None`] entry can still be bound elsewhere.
/// Service bindings are read separately for that reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReportedUsage {
    /// NVOS reports no consumer, or omits the field.
    None,

    /// NVOS lists consumers of the entry.
    Services(Vec<String>),

    /// The field is present in a shape RMS does not recognize.
    Unrecognized,
}

impl ReportedUsage {
    fn from_entry(entry: &Value) -> Self {
        let installed = match entry {
            Value::Null => return Self::None,
            Value::Object(entry) => entry.get("installed").or_else(|| entry.get("[installed]")),
            _ => return Self::Unrecognized,
        };

        let services: Vec<String> = match installed {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::String(service)) if service.is_empty() => Vec::new(),
            Some(Value::String(service)) => vec![service.clone()],
            Some(Value::Object(services)) => services.keys().cloned().collect(),
            Some(Value::Array(services)) => {
                let Some(services) = services
                    .iter()
                    .map(|service| service.as_str().map(ToOwned::to_owned))
                    .collect::<Option<Vec<_>>>()
                else {
                    return Self::Unrecognized;
                };

                services
            }
            Some(_) => return Self::Unrecognized,
        };

        if services.is_empty() {
            Self::None
        } else {
            Self::Services(services)
        }
    }
}

/// The entries one store lists, keyed by ID.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StoreInventory {
    pub entries: BTreeMap<String, ReportedUsage>,
}

impl StoreInventory {
    /// Parses a store collection response: an object keyed by entry ID.
    ///
    /// # Errors
    ///
    /// Returns `Internal` when the response is not a JSON object.
    pub(crate) fn from_collection(store: CertificateStore, response: &Value) -> Result<Self> {
        let entries = match response {
            Value::Null => BTreeMap::new(),
            Value::Object(entries) => entries
                .iter()
                .map(|(id, entry)| (id.clone(), ReportedUsage::from_entry(entry)))
                .collect(),
            _ => {
                return Err(RmsError::internal(format!(
                    "NVOS {store} store listing is not a JSON object"
                )));
            }
        };

        Ok(Self { entries })
    }

    fn needs_space(&self, limit: usize) -> bool {
        self.entries.len() >= limit
    }
}

/// The entries both stores list.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CertificateInventory {
    pub entity: StoreInventory,
    pub ca: StoreInventory,
}

impl CertificateInventory {
    pub(crate) fn store(&self, store: CertificateStore) -> &StoreInventory {
        match store {
            CertificateStore::Entity => &self.entity,
            CertificateStore::Ca => &self.ca,
        }
    }

    /// Whether either store lacks room for one more import.
    pub(crate) fn needs_space(&self, limit: usize) -> bool {
        CertificateStore::ALL
            .into_iter()
            .any(|store| self.store(store).needs_space(limit))
    }
}

/// A service binding RMS could not confirm.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct UnreadableBinding {
    pub service: &'static str,
    pub reason: String,
}

/// Which entries of one store the switch services are bound to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct StoreBindings {
    /// Entry ID to the services bound to it.
    pub bound: BTreeMap<String, BTreeSet<&'static str>>,

    /// Services whose binding could not be confirmed.
    pub unreadable: Vec<UnreadableBinding>,
}

/// Which entries of both stores the switch services are bound to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CertificateBindings {
    pub entity: StoreBindings,
    pub ca: StoreBindings,
}

impl CertificateBindings {
    pub(crate) fn store(&self, store: CertificateStore) -> &StoreBindings {
        match store {
            CertificateStore::Entity => &self.entity,
            CertificateStore::Ca => &self.ca,
        }
    }

    /// Records one service's binding read for `store`: the entry IDs it is
    /// bound to, or why the binding could not be confirmed.
    pub(crate) fn record(
        &mut self,
        store: CertificateStore,
        service: SwitchMtlsService,
        read: std::result::Result<Vec<String>, String>,
    ) {
        let bindings = match store {
            CertificateStore::Entity => &mut self.entity,
            CertificateStore::Ca => &mut self.ca,
        };

        match read {
            Ok(ids) => {
                for id in ids {
                    bindings
                        .bound
                        .entry(id)
                        .or_default()
                        .insert(service.as_str());
                }
            }
            Err(reason) => bindings.unreadable.push(UnreadableBinding {
                service: service.as_str(),
                reason,
            }),
        }
    }
}

/// Reads the entry IDs a service configuration response binds through
/// `field`.
///
/// The top level and the `applied` and `operational` views are all read,
/// because they can name different entries while a revision is applying. An
/// absent or null field means the service is not bound through it.
///
/// # Errors
///
/// Returns why the binding could not be confirmed when the response or a view
/// is not a JSON object, or when the field is present but not a string.
pub(crate) fn bound_entry_ids(
    response: &Value,
    field: &str,
) -> std::result::Result<Vec<String>, String> {
    if !response.is_object() {
        return Err("response is not a JSON object".to_owned());
    }

    let views = [
        ("response", Some(response)),
        ("applied", response.get("applied")),
        ("operational", response.get("operational")),
    ];

    let mut ids = BTreeSet::new();

    for (name, view) in views {
        let Some(view) = view else {
            continue;
        };

        if !view.is_object() {
            return Err(format!("{name} view is not a JSON object"));
        }

        match view.get(field) {
            None | Some(Value::Null) => {}
            Some(Value::String(id)) if id.is_empty() => {}
            Some(Value::String(id)) => {
                ids.insert(id.clone());
            }
            Some(_) => return Err(format!("{field} is not a string")),
        }
    }

    Ok(ids.into_iter().collect())
}

/// The planned reclamation for one store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct StoreReclaimPlan {
    pub store: CertificateStore,
    pub limit: usize,
    pub entries: usize,
    pub rms_entries: usize,

    /// Entries to delete, oldest first. Empty when the store has room.
    pub deletions: Vec<String>,
}

/// When RMS created `id`, from its timestamp suffix.
fn import_time(id: &str) -> Option<NaiveDateTime> {
    let suffix = id.get(id.len().checked_sub(IMPORT_TIMESTAMP_LEN)?..)?;

    NaiveDateTime::parse_from_str(suffix, REMOTE_RUN_ID_FORMAT).ok()
}

/// Plans the deletions that give `store` room for one import.
///
/// Candidates are taken oldest first by their import timestamp. RMS-prefixed
/// IDs without a readable timestamp sort before timestamped ones.
///
/// # Errors
///
/// Returns `FailedPrecondition` when the store needs room and either a
/// binding could not be confirmed or too few entries are safe to delete.
pub(crate) fn plan_store_reclaim(
    store: CertificateStore,
    inventory: &StoreInventory,
    bindings: &StoreBindings,
    protected: &BTreeSet<String>,
    limit: usize,
) -> Result<StoreReclaimPlan> {
    let entries = inventory.entries.len();
    let rms_entries = inventory
        .entries
        .keys()
        .filter(|id| id.starts_with(RMS_CERTIFICATE_ID_PREFIX))
        .count();

    let plan = |deletions| StoreReclaimPlan {
        store,
        limit,
        entries,
        rms_entries,
        deletions,
    };

    let required = (entries + 1).saturating_sub(limit);

    if required == 0 {
        return Ok(plan(Vec::new()));
    }

    if !bindings.unreadable.is_empty() {
        let unreadable = bindings
            .unreadable
            .iter()
            .map(|binding| format!("{}: {}", binding.service, binding.reason))
            .collect::<Vec<_>>()
            .join("; ");

        return Err(RmsError::failed_precondition(format!(
            "NVOS {store} store is full ({entries} of {limit} entries) and RMS could not \
             confirm which entries switch services use ({unreadable}); no entries were deleted"
        )));
    }

    let mut candidates: Vec<&String> = inventory
        .entries
        .iter()
        .filter(|(id, usage)| {
            id.starts_with(RMS_CERTIFICATE_ID_PREFIX)
                && is_valid_identifier(id)
                && !protected.contains(*id)
                && **usage == ReportedUsage::None
                && !bindings.bound.contains_key(*id)
        })
        .map(|(id, _)| id)
        .collect();

    if candidates.len() < required {
        return Err(RmsError::failed_precondition(format!(
            "NVOS {store} store is full ({entries} of {limit} entries, {rms_entries} created by \
             RMS) and {} unused RMS entries can be deleted, {required} needed; delete unused \
             entries on the switch and retry; no entries were deleted",
            candidates.len()
        )));
    }

    candidates.sort_by_key(|id| (import_time(id), *id));

    Ok(plan(
        candidates.into_iter().take(required).cloned().collect(),
    ))
}

/// Where one service's binding for one store is read.
struct BindingSource {
    service: SwitchMtlsService,
    store: CertificateStore,
    endpoint: String,
    field: &'static str,

    /// Whether a 404 means the service has no binding rather than an
    /// unconfirmed one.
    absent_when_not_found: bool,
}

fn binding_sources(service: SwitchMtlsService) -> [BindingSource; 2] {
    let source = |store, endpoint: &str, field, absent_when_not_found| BindingSource {
        service,
        store,
        endpoint: endpoint.to_owned(),
        field,
        absent_when_not_found,
    };

    match service {
        SwitchMtlsService::NvueApi => [
            source(
                CertificateStore::Entity,
                NVUE_SYSTEM_API_PATH,
                "certificate",
                false,
            ),
            source(
                CertificateStore::Ca,
                NVUE_SYSTEM_API_MTLS_PATH,
                "ca-certificate",
                false,
            ),
        ],
        SwitchMtlsService::ScaleUpFabricTelemetryInterface => [
            source(
                CertificateStore::Entity,
                GNMI_SERVER_ENDPOINT,
                "certificate",
                true,
            ),
            source(
                CertificateStore::Ca,
                GNMI_SERVER_MTLS_ENDPOINT,
                "ca-certificate",
                true,
            ),
        ],
        SwitchMtlsService::ScaleUpFabricManager | SwitchMtlsService::ScaleUpFabricTelemetry => {
            let app = service
                .cluster_app_name()
                .expect("scale-up fabric services are cluster apps");

            [
                source(
                    CertificateStore::Entity,
                    &nvue_cluster_app_manager_sub_endpoint(app, "certificate"),
                    "certificate",
                    true,
                ),
                source(
                    CertificateStore::Ca,
                    &nvue_cluster_app_manager_sub_endpoint(app, "ca-certificate"),
                    "ca-certificate",
                    true,
                ),
            ]
        }
    }
}

impl SwitchGb200Nvidia {
    /// Lists both NVOS certificate stores.
    ///
    /// A 404 for a store collection reads as an empty store.
    ///
    /// # Errors
    ///
    /// Returns the transport's error, or `Internal` for a listing RMS does not
    /// recognize.
    pub(crate) async fn read_certificate_inventory(&self) -> Result<CertificateInventory> {
        if self.mtls_skip_nvue_for_test() {
            return Ok(CertificateInventory::default());
        }

        Ok(CertificateInventory {
            entity: self
                .read_certificate_store(CertificateStore::Entity)
                .await?,
            ca: self.read_certificate_store(CertificateStore::Ca).await?,
        })
    }

    async fn read_certificate_store(&self, store: CertificateStore) -> Result<StoreInventory> {
        match self
            .get_certificate_document(store.collection_endpoint())
            .await
        {
            Ok(response) => StoreInventory::from_collection(store, &response),
            Err(error) if error.code == ErrorCode::NotFound => Ok(StoreInventory::default()),
            Err(error) => Err(RmsError::new(
                error.code,
                format!("failed to list the NVOS {store} store: {}", error.message),
            )),
        }
    }

    /// GETs a store listing or service binding.
    ///
    /// A successful response without a body is an error rather than an empty
    /// object, so a missing document can never read as "no entries" or "no
    /// binding". A JSON `{}` or `null` body is returned as sent.
    async fn get_certificate_document(&self, endpoint: &str) -> Result<Value> {
        let response = self
            .nvue_client()?
            .get(endpoint, HttpClient::DEFAULT_TIMEOUT)
            .await?;

        if response.is_success() && response.body.trim().is_empty() {
            return Err(RmsError::internal(format!(
                "NVUE GET {endpoint} returned {} without a body",
                response.status
            )));
        }

        Ok(response.into_json("GET", endpoint)?)
    }

    /// Reads every switch service's binding for both stores.
    ///
    /// Read failures are recorded as unconfirmed bindings rather than
    /// returned, so a store that never needs reclaiming is unaffected by them.
    pub(crate) async fn read_certificate_bindings(&self) -> CertificateBindings {
        let mut bindings = CertificateBindings::default();

        if self.mtls_skip_nvue_for_test() {
            return bindings;
        }

        for source in SwitchMtlsService::ALL.into_iter().flat_map(binding_sources) {
            let read = match self.get_certificate_document(&source.endpoint).await {
                Ok(response) => bound_entry_ids(&response, source.field),
                Err(error) if error.code == ErrorCode::NotFound && source.absent_when_not_found => {
                    Ok(Vec::new())
                }
                Err(error) => Err(format!("GET {}: {}", source.endpoint, error.message)),
            };

            bindings.record(source.store, source.service, read);
        }

        bindings
    }

    /// Deletes one RMS-created entry from an NVOS certificate store.
    ///
    /// NVUE has no HTTP DELETE for store entries; it deletes them through the
    /// entry's `@delete` action, and RMS waits for that action to finish. A
    /// 404 counts as deleted; the caller re-lists the store afterwards.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` for an ID RMS did not create, `Internal`
    /// when a successful response carries no action ID to wait on, or the
    /// switch's error when it refuses the deletion.
    pub(crate) async fn delete_certificate_entry(
        &self,
        store: CertificateStore,
        id: &str,
    ) -> Result<()> {
        if !id.starts_with(RMS_CERTIFICATE_ID_PREFIX) || !is_valid_identifier(id) {
            return Err(RmsError::invalid_argument(format!(
                "refusing to delete NVOS {store} entry {id}: not an RMS-created ID"
            )));
        }

        let endpoint = store.entry_endpoint(id);
        let payload = BTreeMap::from([(
            NVUE_DELETE_ACTION,
            SimpleAction::<()>::start_without_parameters(),
        )]);

        let response = self
            .nvue_client()?
            .post(&endpoint, &payload, HttpClient::DEFAULT_TIMEOUT)
            .await?;

        if !response.is_success() && response.status != 404 {
            let message = if response.body.trim().is_empty() {
                format!(
                    "NVUE POST {endpoint} {NVUE_DELETE_ACTION} returned {}",
                    response.status
                )
            } else {
                format!(
                    "NVUE POST {endpoint} {NVUE_DELETE_ACTION} returned {}: {}",
                    response.status, response.body
                )
            };

            return Err(RmsError::new(
                rms_http_status_code(response.status),
                message,
            ));
        }

        if response.is_success() {
            let action_id = extract_job_id(&response.value).map_err(|error| {
                RmsError::internal(format!(
                    "NVUE POST {endpoint} {NVUE_DELETE_ACTION} returned {} without an action \
                     ID, so the deletion cannot be confirmed: {}",
                    response.status, error.message
                ))
            })?;

            self.wait_for_nvue_action_success(&action_id).await?;
        }

        tracing::info!(
            node = %self.id(),
            %store,
            certificate_id = %id,
            status = response.status,
            "deleted unused RMS certificate-store entry"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const LIMIT: usize = NVOS_CERTIFICATE_STORE_LIMIT;

    fn rms_id(kind: &str, second: u32) -> String {
        format!("rms-fabric-a-{kind}-2026-01-01_00-00-{second:02}_000")
    }

    fn inventory(ids: impl IntoIterator<Item = String>) -> StoreInventory {
        StoreInventory {
            entries: ids
                .into_iter()
                .map(|id| (id, ReportedUsage::None))
                .collect(),
        }
    }

    fn bound(pairs: &[(&str, &'static str)]) -> StoreBindings {
        let mut bindings = StoreBindings::default();
        for (id, service) in pairs {
            bindings
                .bound
                .entry((*id).to_owned())
                .or_default()
                .insert(*service);
        }
        bindings
    }

    #[test]
    fn reported_usage_reads_every_installed_shape() {
        let cases = [
            (json!({}), ReportedUsage::None),
            (json!(null), ReportedUsage::None),
            (json!({"installed": []}), ReportedUsage::None),
            (json!({"installed": ""}), ReportedUsage::None),
            (
                json!({"installed": ["gnmi-server"]}),
                ReportedUsage::Services(vec!["gnmi-server".into()]),
            ),
            (
                json!({"[installed]": {"nvue-rest-api": {}}}),
                ReportedUsage::Services(vec!["nvue-rest-api".into()]),
            ),
            (
                json!({"installed": "nvue-rest-api"}),
                ReportedUsage::Services(vec!["nvue-rest-api".into()]),
            ),
            (json!({"installed": [1]}), ReportedUsage::Unrecognized),
            (json!({"installed": true}), ReportedUsage::Unrecognized),
            (json!("entry"), ReportedUsage::Unrecognized),
        ];

        for (entry, expected) in cases {
            assert_eq!(ReportedUsage::from_entry(&entry), expected, "{entry}");
        }
    }

    #[test]
    fn store_listing_must_be_an_object() {
        let listed = StoreInventory::from_collection(
            CertificateStore::Ca,
            &json!({"rms-a": {}, "site-root": {"installed": ["x"]}}),
        )
        .unwrap();
        assert_eq!(listed.entries.len(), 2);

        let error =
            StoreInventory::from_collection(CertificateStore::Ca, &json!(["rms-a"])).unwrap_err();
        assert_eq!(error.code, ErrorCode::Internal);
    }

    #[test]
    fn bound_entry_ids_reads_every_view_and_rejects_unknown_shapes() {
        assert_eq!(
            bound_entry_ids(
                &json!({"certificate": "a", "applied": {"certificate": "b"}, "operational": {}}),
                "certificate",
            ),
            Ok(vec!["a".to_owned(), "b".to_owned()])
        );
        assert_eq!(bound_entry_ids(&json!({}), "certificate"), Ok(vec![]));
        assert!(bound_entry_ids(&json!({"certificate": 7}), "certificate").is_err());
        assert!(bound_entry_ids(&json!({"applied": "x"}), "certificate").is_err());
        assert!(bound_entry_ids(&json!("certificate"), "certificate").is_err());
    }

    #[test]
    fn a_store_with_room_plans_no_deletions_even_with_unreadable_bindings() {
        let bindings = StoreBindings {
            unreadable: vec![UnreadableBinding {
                service: "nvue-api",
                reason: "timeout".into(),
            }],
            ..StoreBindings::default()
        };

        let plan = plan_store_reclaim(
            CertificateStore::Entity,
            &inventory((0..LIMIT as u32 - 1).map(|s| rms_id("cert", s))),
            &bindings,
            &BTreeSet::new(),
            LIMIT,
        )
        .unwrap();

        assert!(plan.deletions.is_empty());
    }

    #[test]
    fn a_full_store_deletes_its_oldest_unused_rms_entry() {
        let ids: Vec<String> = (0..LIMIT as u32).map(|s| rms_id("cert", s)).collect();
        let bindings = bound(&[(&ids[0], "nvue-api")]);

        let plan = plan_store_reclaim(
            CertificateStore::Entity,
            &inventory(ids.clone()),
            &bindings,
            &BTreeSet::new(),
            LIMIT,
        )
        .unwrap();

        assert_eq!(plan.deletions, vec![ids[1].clone()]);
    }

    #[test]
    fn over_capacity_deletes_enough_to_fit_one_import() {
        let ids: Vec<String> = (0..LIMIT as u32 + 2).map(|s| rms_id("ca", s)).collect();

        let plan = plan_store_reclaim(
            CertificateStore::Ca,
            &inventory(ids.clone()),
            &StoreBindings::default(),
            &BTreeSet::new(),
            LIMIT,
        )
        .unwrap();

        assert_eq!(plan.deletions, ids[..3].to_vec());
    }

    #[test]
    fn skips_rms_prefixed_ids_that_cannot_be_deleted() {
        let ids: Vec<String> = (0..LIMIT as u32 - 1).map(|s| rms_id("cert", s)).collect();
        let mut store = inventory(ids.clone());
        store
            .entries
            .insert("rms-by hand".into(), ReportedUsage::None);

        let plan = plan_store_reclaim(
            CertificateStore::Entity,
            &store,
            &StoreBindings::default(),
            &BTreeSet::new(),
            LIMIT,
        )
        .unwrap();

        assert_eq!(plan.deletions, vec![ids[0].clone()]);
    }

    #[test]
    fn never_deletes_non_rms_protected_in_use_or_unrecognized_entries() {
        let mut store = inventory((0..LIMIT as u32 - 4).map(|s| rms_id("cert", s)));
        store
            .entries
            .insert("site-cert".into(), ReportedUsage::None);
        store.entries.insert(
            rms_id("cert", 50),
            ReportedUsage::Services(vec!["gnmi-server".into()]),
        );
        store
            .entries
            .insert(rms_id("cert", 51), ReportedUsage::Unrecognized);
        store
            .entries
            .insert(rms_id("cert", 52), ReportedUsage::None);

        let bindings = StoreBindings {
            bound: (0..LIMIT as u32 - 4)
                .map(|s| {
                    (
                        rms_id("cert", s),
                        BTreeSet::from(["scale-up-fabric-manager"]),
                    )
                })
                .collect(),
            ..StoreBindings::default()
        };

        let protected = BTreeSet::from([rms_id("cert", 52)]);

        let error = plan_store_reclaim(
            CertificateStore::Entity,
            &store,
            &bindings,
            &protected,
            LIMIT,
        )
        .unwrap_err();

        assert_eq!(error.code, ErrorCode::FailedPrecondition);
        assert!(
            error.message.contains("0 unused RMS entries"),
            "{}",
            error.message
        );
        assert!(error.message.contains("no entries were deleted"));
    }

    #[test]
    fn an_unreadable_binding_blocks_deletion_in_a_full_store() {
        let bindings = StoreBindings {
            unreadable: vec![UnreadableBinding {
                service: "scale-up-fabric-telemetry",
                reason: "GET /x: timed out".into(),
            }],
            ..StoreBindings::default()
        };

        let error = plan_store_reclaim(
            CertificateStore::Ca,
            &inventory((0..LIMIT as u32).map(|s| rms_id("ca", s))),
            &bindings,
            &BTreeSet::new(),
            LIMIT,
        )
        .unwrap_err();

        assert_eq!(error.code, ErrorCode::FailedPrecondition);
        assert!(
            error
                .message
                .contains("scale-up-fabric-telemetry: GET /x: timed out")
        );
    }

    #[test]
    fn rms_ids_without_a_timestamp_sort_as_oldest() {
        let mut ids: Vec<String> = (0..LIMIT as u32 - 1).map(|s| rms_id("ca", s)).collect();
        ids.push("rms-legacy-ca".into());

        let plan = plan_store_reclaim(
            CertificateStore::Ca,
            &inventory(ids),
            &StoreBindings::default(),
            &BTreeSet::new(),
            LIMIT,
        )
        .unwrap();

        assert_eq!(plan.deletions, vec!["rms-legacy-ca".to_owned()]);
    }

    #[tokio::test]
    async fn reads_bindings_for_every_service_including_absent_apps() {
        let server = MockServer::start().await;

        let responses = [
            (
                NVUE_SYSTEM_API_PATH,
                200,
                json!({"certificate": "rms-cert-api"}),
            ),
            (
                NVUE_SYSTEM_API_MTLS_PATH,
                200,
                json!({"ca-certificate": "rms-ca-api"}),
            ),
            (
                GNMI_SERVER_ENDPOINT,
                200,
                json!({"certificate": "rms-cert-gnmi"}),
            ),
            (GNMI_SERVER_MTLS_ENDPOINT, 404, json!({})),
            (
                "/nvue_v1/cluster/apps/nmx-controller/manager/certificate",
                200,
                json!({"certificate": "rms-cert-old"}),
            ),
            (
                "/nvue_v1/cluster/apps/nmx-controller/manager/ca-certificate",
                200,
                json!({"ca-certificate": "rms-ca-old"}),
            ),
            (
                "/nvue_v1/cluster/apps/nmx-telemetry/manager/certificate",
                404,
                json!({}),
            ),
            (
                "/nvue_v1/cluster/apps/nmx-telemetry/manager/ca-certificate",
                500,
                json!({}),
            ),
        ];

        for (endpoint, status, body) in responses {
            Mock::given(method("GET"))
                .and(path(endpoint))
                .respond_with(ResponseTemplate::new(status).set_body_json(body))
                .mount(&server)
                .await;
        }

        let switch = SwitchGb200Nvidia::for_test(&server.uri());
        let bindings = switch.read_certificate_bindings().await;

        assert_eq!(
            bindings.entity.bound.keys().collect::<Vec<_>>(),
            ["rms-cert-api", "rms-cert-gnmi", "rms-cert-old"]
        );
        assert!(bindings.entity.unreadable.is_empty());

        assert_eq!(
            bindings.ca.bound.keys().collect::<Vec<_>>(),
            ["rms-ca-api", "rms-ca-old"]
        );
        assert_eq!(bindings.ca.unreadable.len(), 1);
        assert_eq!(
            bindings.ca.unreadable[0].service,
            "scale-up-fabric-telemetry"
        );
    }

    #[tokio::test]
    async fn a_binding_response_without_a_body_is_unconfirmed() {
        let server = MockServer::start().await;
        let manager_certificate = "/nvue_v1/cluster/apps/nmx-controller/manager/certificate";

        Mock::given(method("GET"))
            .and(path(manager_certificate))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());
        let bindings = switch.read_certificate_bindings().await;

        assert!(bindings.entity.bound.is_empty());
        assert_eq!(bindings.entity.unreadable.len(), 1);
        assert_eq!(
            bindings.entity.unreadable[0].service,
            SwitchMtlsService::ScaleUpFabricManager.as_str()
        );
        assert!(
            bindings.entity.unreadable[0]
                .reason
                .contains("without a body"),
            "{}",
            bindings.entity.unreadable[0].reason
        );
        assert!(bindings.ca.unreadable.is_empty());
    }

    #[tokio::test]
    async fn a_store_listing_without_a_body_fails_but_null_reads_as_empty() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path(NVUE_ENTITY_CERTIFICATE_COLLECTION))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(NVUE_CA_CERTIFICATE_COLLECTION))
            .respond_with(ResponseTemplate::new(200).set_body_string("null"))
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        let error = switch.read_certificate_inventory().await.unwrap_err();
        assert!(
            error.message.contains("entity-certificate"),
            "{}",
            error.message
        );
        assert!(
            error.message.contains("without a body"),
            "{}",
            error.message
        );

        let ca = switch
            .read_certificate_store(CertificateStore::Ca)
            .await
            .unwrap();
        assert!(ca.entries.is_empty());
    }

    #[tokio::test]
    async fn a_missing_nvue_api_binding_is_unconfirmed_not_absent() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());
        let bindings = switch.read_certificate_bindings().await;

        for store in CertificateStore::ALL {
            let unreadable: Vec<_> = bindings
                .store(store)
                .unreadable
                .iter()
                .map(|binding| binding.service)
                .collect();
            assert_eq!(unreadable, ["nvue-api"], "{store}");
        }
    }

    #[tokio::test]
    async fn lists_both_stores_and_reads_a_missing_collection_as_empty() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path(NVUE_ENTITY_CERTIFICATE_COLLECTION))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"rms-a": {"installed": ["nvue-rest-api"]}, "site": {}})),
            )
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(NVUE_CA_CERTIFICATE_COLLECTION))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());
        let inventory = switch.read_certificate_inventory().await.unwrap();

        assert_eq!(inventory.entity.entries.len(), 2);
        assert_eq!(
            inventory.entity.entries["rms-a"],
            ReportedUsage::Services(vec!["nvue-rest-api".into()])
        );
        assert!(inventory.ca.entries.is_empty());
    }

    #[tokio::test]
    async fn a_failed_store_listing_keeps_its_error_code() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());
        let error = switch.read_certificate_inventory().await.unwrap_err();

        assert_eq!(error.code, ErrorCode::Unavailable);
        assert!(error.message.contains("entity-certificate"));
    }

    async fn mount_delete_action(
        server: &MockServer,
        collection: &str,
        id: &str,
        response: ResponseTemplate,
    ) {
        Mock::given(method("POST"))
            .and(path(format!("{collection}/{id}")))
            .and(body_json(json!({ "@delete": { "state": "start" } })))
            .respond_with(response)
            .expect(1)
            .mount(server)
            .await;
    }

    async fn mount_action_status(server: &MockServer, action_id: &str, status: Value) {
        Mock::given(method("GET"))
            .and(path(format!("/nvue_v1/action/{action_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(status))
            .expect(1)
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn a_successful_delete_without_an_action_id_fails() {
        let server = MockServer::start().await;

        mount_delete_action(
            &server,
            NVUE_CA_CERTIFICATE_COLLECTION,
            "rms-accepted",
            ResponseTemplate::new(202).set_body_json(json!({})),
        )
        .await;

        mount_delete_action(
            &server,
            NVUE_CA_CERTIFICATE_COLLECTION,
            "rms-empty",
            ResponseTemplate::new(200),
        )
        .await;

        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"state": "action_success"})),
            )
            .expect(0)
            .named("action status poll")
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        for (id, status) in [("rms-accepted", 202), ("rms-empty", 200)] {
            let error = switch
                .delete_certificate_entry(CertificateStore::Ca, id)
                .await
                .unwrap_err();

            assert_eq!(error.code, ErrorCode::Internal);
            assert!(
                error
                    .message
                    .contains(&format!("returned {status} without an action ID")),
                "{}",
                error.message
            );
        }
    }

    #[tokio::test]
    async fn deletion_waits_for_action_success_despite_reboot_hints() {
        let server = MockServer::start().await;

        mount_delete_action(
            &server,
            NVUE_CA_CERTIFICATE_COLLECTION,
            "rms-old",
            ResponseTemplate::new(201).set_body_json("delete-ca"),
        )
        .await;

        Mock::given(method("GET"))
            .and(path("/nvue_v1/action/delete-ca"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    json!({"state": "action_running", "status": "Reboot in progress"}),
                ),
            )
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;
        mount_action_status(&server, "delete-ca", json!({"state": "action_success"})).await;

        mount_delete_action(
            &server,
            NVUE_CA_CERTIFICATE_COLLECTION,
            "rms-rebooting",
            ResponseTemplate::new(201).set_body_json("delete-rebooting"),
        )
        .await;
        mount_action_status(
            &server,
            "delete-rebooting",
            json!({"state": "action_error", "detail": "system will reboot"}),
        )
        .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        switch
            .delete_certificate_entry(CertificateStore::Ca, "rms-old")
            .await
            .unwrap();

        let error = switch
            .delete_certificate_entry(CertificateStore::Ca, "rms-rebooting")
            .await
            .unwrap_err();
        assert!(
            error
                .message
                .contains("failed with state action_error: system will reboot"),
            "{}",
            error.message
        );
    }

    #[tokio::test]
    async fn deletes_through_the_nvue_delete_action_and_refuses_other_ids() {
        let server = MockServer::start().await;

        mount_delete_action(
            &server,
            NVUE_ENTITY_CERTIFICATE_COLLECTION,
            "rms-old",
            ResponseTemplate::new(201).set_body_json("delete-entity"),
        )
        .await;
        mount_action_status(&server, "delete-entity", json!({"state": "action_success"})).await;

        mount_delete_action(
            &server,
            NVUE_CA_CERTIFICATE_COLLECTION,
            "rms-old",
            ResponseTemplate::new(201).set_body_json("delete-ca"),
        )
        .await;
        mount_action_status(&server, "delete-ca", json!({"state": "action_success"})).await;

        mount_delete_action(
            &server,
            NVUE_CA_CERTIFICATE_COLLECTION,
            "rms-gone",
            ResponseTemplate::new(404),
        )
        .await;

        mount_delete_action(
            &server,
            NVUE_CA_CERTIFICATE_COLLECTION,
            "rms-busy",
            ResponseTemplate::new(409).set_body_string("certificate in use"),
        )
        .await;

        mount_delete_action(
            &server,
            NVUE_CA_CERTIFICATE_COLLECTION,
            "rms-refused",
            ResponseTemplate::new(201).set_body_json("delete-refused"),
        )
        .await;
        mount_action_status(
            &server,
            "delete-refused",
            json!({
                "state": "action_error",
                "issue": [{
                    "code": "generic",
                    "message": "certificate is installed",
                    "severity": "error",
                }],
            }),
        )
        .await;

        Mock::given(method("DELETE"))
            .respond_with(ResponseTemplate::new(405))
            .expect(0)
            .named("HTTP DELETE of a store entry")
            .mount(&server)
            .await;

        let switch = SwitchGb200Nvidia::for_test(&server.uri());

        for store in [CertificateStore::Entity, CertificateStore::Ca] {
            switch
                .delete_certificate_entry(store, "rms-old")
                .await
                .unwrap();
        }

        switch
            .delete_certificate_entry(CertificateStore::Ca, "rms-gone")
            .await
            .unwrap();

        let refused = switch
            .delete_certificate_entry(CertificateStore::Ca, "rms-busy")
            .await
            .unwrap_err();
        assert_eq!(refused.code, ErrorCode::AlreadyExists);
        assert!(refused.message.contains("@delete"));
        assert!(refused.message.contains("certificate in use"));

        let failed = switch
            .delete_certificate_entry(CertificateStore::Ca, "rms-refused")
            .await
            .unwrap_err();
        assert!(
            failed.message.contains("action_error"),
            "{}",
            failed.message
        );

        let rejected = switch
            .delete_certificate_entry(CertificateStore::Ca, "site-root")
            .await
            .unwrap_err();
        assert_eq!(rejected.code, ErrorCode::InvalidArgument);
    }
}
