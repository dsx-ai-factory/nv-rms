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

use nv_redfish::core::{EntityTypeRef, ODataETag, ODataId};
use serde::Deserialize;

/// JSON Redfish entity fetched through nv-redfish so URI resolution and
/// credential attachment stay origin-pinned to the configured BMC.
///
/// `T` is the resource body after `@odata.id` / `@odata.etag` are peeled off
/// for [`EntityTypeRef`]. Use a struct for typed fields, or [`serde_json::Value`]
/// when the caller must accept vendor keys that are not modeled yet.
#[derive(Debug, Clone, Deserialize)]
pub(super) struct RedfishJson<T> {
    #[serde(rename = "@odata.id", default = "empty_odata_id")]
    odata_id: ODataId,
    #[serde(rename = "@odata.etag")]
    etag: Option<ODataETag>,
    #[serde(flatten)]
    body: T,
}

impl<T> RedfishJson<T> {
    pub(super) fn into_body(self) -> T {
        self.body
    }
}

impl<T: Send + Sync> EntityTypeRef for RedfishJson<T> {
    fn odata_id(&self) -> &ODataId {
        &self.odata_id
    }

    fn etag(&self) -> Option<&ODataETag> {
        self.etag.as_ref()
    }
}

/// Redfish `{ "@odata.id": "..." }` navigation link.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
pub struct ODataLink {
    #[serde(rename = "@odata.id", default)]
    pub odata_id: String,
}

impl ODataLink {
    /// Returns the link path when it is a non-empty string.
    pub fn nonempty(&self) -> Option<&str> {
        if self.odata_id.is_empty() {
            None
        } else {
            Some(self.odata_id.as_str())
        }
    }
}

/// Redfish collection whose `Members` are `@odata.id` links.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ResourceCollection {
    #[serde(rename = "Members", default)]
    pub members: Vec<ODataLink>,
}

fn empty_odata_id() -> ODataId {
    ODataId::from(String::new())
}
