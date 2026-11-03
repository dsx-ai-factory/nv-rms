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

//! Runtime NVOS behavior classified on the live switch.
//!
//! # The problem
//!
//! RMS talks to NVOS over NVUE for two different jobs: binding switch
//! certificates (including gNMI) and bringing up scale-up fabric (including
//! NVBridge / secondary node IPs).
//!
//! Those jobs assume a single NVOS surface. They do not have one. Some images
//! expose a gNMI entity certificate but not a gNMI client-CA tree. Some images
//! have NVBridge secondary-node APIs; others do not. A missing resource looks
//! like HTTP 404. A sick or unreachable switch looks like 5xx, timeout, or
//! auth failure. Those are not the same event.
//!
//! If RMS treats "this URI is absent" as a workflow failure, a cert job can
//! abort after a partial commit, or a fabric job can fail for a gNMI problem
//! it did not need. If it pretends the resource always exists, it will PATCH
//! something the image does not have. If it guesses from NVOS version strings,
//! it will skip work that the live switch can do, or attempt work the live
//! switch cannot.
//!
//! RMS must complete rack operations correctly across NVOS images that do not
//! share the same NVUE features, without turning optional southbound APIs into
//! hard failures or using a version table as a stand-in for what the switch
//! actually implements.
//!
//! # Approach
//!
//! Possible ways to resolve this, corresponding to earlier experiments:
//!
//! 1. **Version-ranged ops table.** Parse `current_build_id` and stitch a
//!    table of async function pointers from defaults plus version/platform
//!    patches. Handlers call the table. Optional features are fail-closed
//!    ranges (for example gNMI mTLS only above certain 25.02 / 25.03
//!    builds). One slot per workflow is easy to overlay, but version is a
//!    poor stand-in for "does this URI exist," and boxed-fn dispatch is
//!    heavy for little real branching.
//! 2. **Version-ranged policy enums.** Same rules and ranges, but a small
//!    policy struct and ordinary `match` in async methods instead of a
//!    pointer table. Simpler than (1); still the wrong gate if the ranges
//!    do not match the fleet.
//! 3. **Tactical live bind, no version table.** Always PATCH the gNMI entity
//!    certificate; PATCH `/gnmi-server/mtls` only when GET shows it exists.
//!    404 skips client-CA; 5xx fails. Fixes the two-PATCH / dangling-revision
//!    bug and matches the fleet, but is gNMI-only and does not classify
//!    NVBridge, so further 404 checks tend to accumulate ad hoc.
//! 4. **Evidence-based classification factory (this module).** Classification
//!    uses whatever predicates that workflow actually needs (an NVUE probe,
//!    a build-id check, both, or some other fact). Certificate jobs use
//!    [`GnmiClientCa::discover`]; fabric uses [`NvBridge::discover`]. Each
//!    factory gathers only the observations that workflow needs and returns
//!    that capability alone, so an unprobed answer cannot be mistaken for a
//!    404. 404 is unsupported; 5xx / timeout / auth fails discovery.
//!
//! # Classification
//!
//! This module is platform-neutral: GB200 and GB300 (and later NVOS switches)
//! implement [`NvosHost`]. Callers hold the capability they discovered
//! ([`GnmiClientCa`] or [`NvBridge`]) and ask [`GnmiClientCa::is_supported`]
//! or [`NvBridge::is_supported`]. They never branch on image strings, GET
//! paths, or version tables. Certificate initialization calls
//! [`GnmiClientCa::discover`]; fabric calls [`NvBridge::discover`]. Each
//! gathers only the
//! [`Evidence`] that workflow reads, then classifies the capability as the
//! **conjunction** of its requirement predicates. Later bind, verify, and
//! fabric steps use the named capability functions, not the observations that
//! produced them.
//!
//! # Extending discovery
//!
//! Capabilities differ in *how* they are known, not in how workflows consume
//! them. Classification is whatever you can record on [`Evidence`]: an NVUE
//! probe (today), a build-id / version check, both, or some other fact
//! (even wall-clock, if that were ever the right signal). A capability can
//! be probe-only, version-only, or the conjunction of several requirements.
//! Adding another kind of check does **not** belong in cert or fabric
//! handlers. Do this instead:
//!
//! 1. Add a field to [`Evidence`] for the new observation (another NVUE GET,
//!    current system-image build id, an SSH fact, …). Load it in
//!    the matching `discover` path. Gather only what that workflow reads: a
//!    5xx on an unused GET still fails the job.
//! 2. Add a `fn(&Evidence) -> bool` predicate next to the existing ones
//!    (`gnmi_mtls_resource`, `secondary_resource`).
//! 3. Append that function to the capability's `REQUIREMENTS` list
//!    corresponding to its `is_supported` function. Supported means **every**
//!    predicate on that list is true.
//! 4. Keep 404 / missing data as `false` on the predicate (unsupported).
//!    Keep 5xx, timeout, and auth as `Err` from capability discovery.
//!
//! Example: NVBridge that also requires a minimum image keeps a secondary
//! resource predicate and adds a predicate that reads a `current_build`
//! field. A capability that NVUE cannot probe would list only version (or
//! other) predicates. Workflows still call
//! [`NvBridge::is_supported`].
//!
//! [`GnmiClientCa::discover`] does not probe `/cluster/node/secondary` and
//! does not return an NVBridge answer. [`NvBridge::discover`] does not probe
//! gNMI and does not return a client-CA answer. Each uses its own requirement
//! list.
//!
//! # Probe on each discovery
//!
//! `discover` probes the live switch and does not remember an earlier answer.
//! Callers that need the same classification twice on one switch object keep
//! it themselves. Certificate initialization stores the gNMI client-CA answer
//! for the rest of that object's life, so bind and verify share one probe. A
//! later job builds a new switch object and probes again.

use async_trait::async_trait;

use crate::utilities::error::Result;

/// Switch that can supply NVOS observations. Platform nodes implement this;
/// this module does not depend on GB200 specifically.
#[async_trait]
pub(crate) trait NvosHost: Send + Sync {
    /// Whether `GET /system/gnmi-server/mtls` exists. 404 is `Ok(false)`.
    async fn gnmi_mtls_resource_present(&self) -> Result<bool>;

    /// Whether `GET /cluster/node/secondary` exists. 404 is `Ok(false)`.
    async fn secondary_cluster_node_present(&self) -> Result<bool>;
}

/// One observation used to classify a capability.
///
/// Add a new `fn(&Evidence) -> bool` and put it on that capability's
/// `REQUIREMENTS` list. Do not interpret evidence in workflow code.
pub(crate) type Requirement = fn(&Evidence) -> bool;

/// Live observations for one switch session.
///
/// Gathered by a capability `discover` path. Fields are facts, not policy:
/// whether a resource existed, what build id NVUE reported, and so on.
/// `None` means this session did not gather that observation. Policy is the
/// per-capability `REQUIREMENTS` list.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Evidence {
    gnmi_mtls_present: Option<bool>,
    secondary_present: Option<bool>,
}

fn requirements_met(evidence: &Evidence, requirements: &[Requirement]) -> bool {
    requirements
        .iter()
        .copied()
        .all(|requirement| requirement(evidence))
}

fn gnmi_mtls_resource(evidence: &Evidence) -> bool {
    evidence.gnmi_mtls_present == Some(true)
}

fn secondary_resource(evidence: &Evidence) -> bool {
    evidence.secondary_present == Some(true)
}

/// Whether this NVOS image implements gNMI client-CA (`/system/gnmi-server/mtls`).
///
/// Produced only by [`GnmiClientCa::discover`]. It has no NVBridge answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GnmiClientCa {
    /// `GET /system/gnmi-server/mtls` returned 404. Bind the entity cert only.
    Unsupported,
    /// The resource exists. Bind the entity certificate and client CA in one revision.
    Supported,
}

impl GnmiClientCa {
    /// Conjunction of observations that must all hold for client-CA bind.
    ///
    /// Extend this list (and [`Evidence`] if a predicate needs a new fact)
    /// instead of adding version or probe branches to the cert job.
    const REQUIREMENTS: &'static [Requirement] = &[gnmi_mtls_resource];

    fn from_evidence(evidence: &Evidence) -> Self {
        if requirements_met(evidence, Self::REQUIREMENTS) {
            Self::Supported
        } else {
            Self::Unsupported
        }
    }

    /// Probe gNMI client-CA support. Does not classify NVBridge.
    pub(crate) async fn discover(switch: &impl NvosHost) -> Result<Self> {
        let evidence = Evidence {
            gnmi_mtls_present: Some(switch.gnmi_mtls_resource_present().await?),
            secondary_present: None,
        };
        Ok(Self::from_evidence(&evidence))
    }

    /// Whether the probed gNMI client-CA resource exists.
    pub(crate) fn is_supported(self) -> bool {
        self == Self::Supported
    }
}

/// Whether this NVOS image implements the secondary cluster-node interface
/// (`/cluster/node/secondary`) used for NVBridge / eth1 node IPs.
///
/// Produced only by [`NvBridge::discover`]. It has no gNMI client-CA answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NvBridge {
    /// `GET /cluster/node/secondary` returned 404.
    Unsupported,
    /// The resource exists. Secondary node IP checks apply.
    Supported,
}

impl NvBridge {
    /// Conjunction of observations that must all hold for NVBridge / eth1.
    ///
    /// Probe-only today. A version or other gate is another [`Requirement`]
    /// on this list, not a table in the fabric handler.
    const REQUIREMENTS: &'static [Requirement] = &[secondary_resource];

    fn from_evidence(evidence: &Evidence) -> Self {
        if requirements_met(evidence, Self::REQUIREMENTS) {
            Self::Supported
        } else {
            Self::Unsupported
        }
    }

    /// Probe NVBridge support. Does not classify gNMI client-CA.
    pub(crate) async fn discover(switch: &impl NvosHost) -> Result<Self> {
        let evidence = Evidence {
            gnmi_mtls_present: None,
            secondary_present: Some(switch.secondary_cluster_node_present().await?),
        };
        Ok(Self::from_evidence(&evidence))
    }

    /// Whether the probed secondary cluster-node resource exists.
    pub(crate) fn is_supported(self) -> bool {
        self == Self::Supported
    }

    #[cfg(test)]
    pub(crate) fn for_test(supported: bool) -> Self {
        if supported {
            Self::Supported
        } else {
            Self::Unsupported
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Evidence, GnmiClientCa, NvBridge, gnmi_mtls_resource, requirements_met, secondary_resource,
    };

    fn never(_: &Evidence) -> bool {
        false
    }

    fn always(_: &Evidence) -> bool {
        true
    }

    #[test]
    fn empty_requirements_are_vacuously_true() {
        assert!(requirements_met(&Evidence::default(), &[]));
    }

    #[test]
    fn capability_support_is_the_conjunction_of_its_requirements() {
        let evidence = Evidence::default();
        assert!(requirements_met(&evidence, &[always, always]));
        assert!(!requirements_met(&evidence, &[always, never]));
    }

    #[test]
    fn nvbridge_is_the_conjunction_of_its_requirements() {
        assert_eq!(
            NvBridge::from_evidence(&Evidence {
                gnmi_mtls_present: None,
                secondary_present: Some(true),
            }),
            NvBridge::Supported
        );
        assert_eq!(
            NvBridge::from_evidence(&Evidence {
                gnmi_mtls_present: None,
                secondary_present: Some(false),
            }),
            NvBridge::Unsupported
        );
    }

    #[test]
    fn each_capability_reads_only_its_own_observation() {
        let gnmi_only = Evidence {
            gnmi_mtls_present: Some(true),
            secondary_present: None,
        };
        assert_eq!(
            GnmiClientCa::from_evidence(&gnmi_only),
            GnmiClientCa::Supported
        );
        assert_eq!(NvBridge::from_evidence(&gnmi_only), NvBridge::Unsupported);
        assert!(!secondary_resource(&gnmi_only));
    }

    #[test]
    fn gnmi_client_ca_is_the_conjunction_of_its_requirements() {
        assert_eq!(
            GnmiClientCa::from_evidence(&Evidence {
                gnmi_mtls_present: Some(true),
                secondary_present: None,
            }),
            GnmiClientCa::Supported
        );
        assert_eq!(
            GnmiClientCa::from_evidence(&Evidence {
                gnmi_mtls_present: Some(false),
                secondary_present: None,
            }),
            GnmiClientCa::Unsupported
        );
    }

    #[test]
    fn unobserved_evidence_does_not_satisfy_a_resource_requirement() {
        let unobserved = Evidence::default();
        assert!(!gnmi_mtls_resource(&unobserved));
        assert!(!secondary_resource(&unobserved));
        assert_eq!(
            GnmiClientCa::from_evidence(&unobserved),
            GnmiClientCa::Unsupported
        );
        assert_eq!(NvBridge::from_evidence(&unobserved), NvBridge::Unsupported);
    }
}
