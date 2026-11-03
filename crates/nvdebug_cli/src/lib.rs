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
//! Runs the nvdebug log collector as an executable RMS finds by configuration.
//!
//! One invocation is rendered here, the one RFD 0001 commits to:
//! `nvdebug collect --dut-config <file> --collector-id "<ids>" --output <dir>
//! --skip-html-reports --skip-zip --non-interactive`, plus `--dry-run` on the
//! same arguments and `--version` as the availability probe. The targets go
//! through a file and never through argv, because they carry the BMC
//! password and argv is readable by every process on the host.
//!
//! The crate interprets nothing nvdebug writes. A [`Collection`] is the exit
//! status, the output directory, and when the run started. nvdebug's own
//! stdout and stderr are discarded. Exit 0 is not "success": nvdebug's own
//! status files under the output directory say what each collector did, and
//! reading them is a later change.
//!
//! The CLI is the integration surface for now. nvdebug is being ported to
//! Rust as a library and a binary crate, with open-sourcing planned; when
//! that crate is usable, RMS compiles it in and this wrapper goes away.

mod bmc;
mod dut_dir;
mod error;
mod io;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt;
use std::path::{Path, PathBuf};
use std::process::ExitStatus;
use std::time::{Duration, SystemTime};

use secrecy::{ExposeSecret, SecretString};
use serde::Serialize;

pub use bmc::{Bmc, BmcHost};
pub use dut_dir::sweep_stale_dut_dirs;
pub use error::{Detail, Error};
pub use tokio_util::sync::CancellationToken;

/// The nvdebug executable.
#[derive(Debug, Clone)]
pub struct Executable {
    path: PathBuf,
}

impl Executable {
    /// `path` is absolute. A bare name would be searched for on `PATH` by
    /// the spawn, so the file run would depend on the caller's environment.
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, Error> {
        let path = path.into();
        if !path.is_absolute() {
            return Err(Error::new_invalid(
                "executable path",
                "expected an absolute path",
            ));
        }

        Ok(Self { path })
    }
}

/// One device nvdebug collects from: a compute tray, a switch tray, or a
/// power shelf, addressed by its BMC.
#[derive(Debug, Clone)]
pub struct Target {
    name: String,
    bmc: Bmc,
    baseboard: String,
}

impl Target {
    /// `name` is the target's key in the DUT file and becomes a directory
    /// under the output directory. It is kept to letters, digits, `.`, `_`,
    /// and `-`: nvdebug's directory sanitizer maps every other character to
    /// `_`, so two names outside that set can share one directory. `.` and
    /// `..` are refused because nvdebug joins the raw name once. `baseboard`
    /// is nvdebug's platform name, for example `GB200 NVL`; `nvdebug
    /// list-baseboards` prints the accepted ones.
    pub fn new(
        name: impl Into<String>,
        bmc: Bmc,
        baseboard: impl Into<String>,
    ) -> Result<Self, Error> {
        let name = name.into();
        let baseboard = baseboard.into();

        let plain = |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-');
        if name.is_empty() || !name.chars().all(plain) || name == "." || name == ".." {
            return Err(Error::new_invalid(
                "target name",
                "expected letters, digits, '.', '_', or '-', and not '.' or '..'",
            ));
        }
        if name == bmc::DUT_DEFAULTS_KEY {
            return Err(Error::new_invalid(
                "target name",
                "DUT_Defaults is reserved by nvdebug's DUT file",
            ));
        }
        if baseboard.is_empty() {
            return Err(Error::new_invalid(
                "baseboard",
                "expected a non-empty baseboard name",
            ));
        }

        Ok(Self {
            name,
            bmc,
            baseboard,
        })
    }
}

/// A collector in nvdebug's catalog, by id, for example `R1` or `H14`.
///
/// Only the shape is checked here. nvdebug validates ids against its catalog
/// at run time, and the catalog changes per release. The shape check exists
/// because nvdebug drops an unknown id silently when valid ones accompany it,
/// so a typo would collect less than asked without a word.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct CollectorId(String);

impl CollectorId {
    /// Accepts any non-empty id without whitespace or commas. Whether the
    /// id exists is nvdebug's to say.
    pub fn new(id: impl Into<String>) -> Result<Self, Error> {
        let id = id.into();

        if id.is_empty() || id.chars().any(|c| c.is_whitespace() || c == ',') {
            return Err(Error::new_invalid(
                "collector id",
                "expected a non-empty id without whitespace or commas",
            ));
        }

        Ok(Self(id))
    }
}

impl fmt::Display for CollectorId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One `collect` run: targets, collectors, where the files go, and how long
/// the run may take. The timeout has no default on purpose: the RFD's
/// 10-minute objective and the 16-to-23-minute measurement are the caller's
/// trade-off, not this crate's.
#[derive(Debug, Clone)]
pub struct CollectRequest {
    targets: Vec<Target>,
    collectors: Vec<CollectorId>,
    output_dir: PathBuf,
    timeout: Duration,
}

impl CollectRequest {
    /// Refuses what nvdebug or the spawn would misread: no targets, no
    /// collectors, two targets of one name, a relative output directory,
    /// a zero timeout.
    pub fn new(
        targets: Vec<Target>,
        collectors: Vec<CollectorId>,
        output_dir: impl Into<PathBuf>,
        timeout: Duration,
    ) -> Result<Self, Error> {
        if targets.is_empty() {
            return Err(Error::new_invalid(
                "targets",
                "expected at least one target",
            ));
        }
        if collectors.is_empty() {
            return Err(Error::new_invalid(
                "collectors",
                "expected at least one collector id",
            ));
        }
        // Two targets of one name would be two keys of one name in the DUT
        // file, which nvdebug's loader refuses.
        let mut names: Vec<&str> = targets.iter().map(|t| t.name.as_str()).collect();
        names.sort_unstable();
        if names.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(Error::new_invalid(
                "targets",
                "expected distinct target names",
            ));
        }
        // The spawn sets no working directory, so a relative path would
        // resolve against RMS's own, which the caller cannot see.
        let output_dir = output_dir.into();
        if !output_dir.is_absolute() {
            return Err(Error::new_invalid(
                "output directory",
                "expected an absolute path",
            ));
        }
        if timeout.is_zero() {
            return Err(Error::new_invalid(
                "timeout",
                "expected a non-zero duration",
            ));
        }

        Ok(Self {
            targets,
            collectors,
            output_dir,
            timeout,
        })
    }
}

/// What a finished `collect` or `dry_run` left behind. The status is
/// nvdebug's own exit status, uninterpreted. `output_dir` held no nvdebug
/// run before this one, and no other run of this crate could use it during
/// this one, so the one `nvdebug_logs_*` directory in it is this run's. The
/// exception is an nvdebug left running by a caller killed outright, which
/// the lock no longer covers. `started` is taken just before the spawn.
#[derive(Debug)]
#[non_exhaustive]
pub struct Collection {
    pub status: ExitStatus,
    pub output_dir: PathBuf,
    pub started: SystemTime,
}

/// One target's entry in the DUT file. The strings are borrowed from the
/// [`Target`], so the password is copied once, into the rendered text.
#[derive(Serialize)]
struct DutEntry<'a> {
    baseboard: &'a str,
    bmc_ip: String,
    bmc_user: &'a str,
    bmc_pass: &'a str,
    #[serde(rename = "IP_NETWORK", skip_serializing_if = "Option::is_none")]
    ip_network: Option<&'static str>,
}

/// Renders the DUT file: a mapping keyed by target name, each value the
/// target's settings. nvdebug takes the name from the key. The text is JSON,
/// which the YAML loader on nvdebug's side accepts, so no YAML writer is
/// needed and every value is quoted correctly whatever the password contains.
///
/// nvdebug's address family defaults to IPv4 and refuses an IPv6 address
/// under it, so an IPv6 address carries its own family. The address is
/// written without brackets: ipmitool takes `bmc_ip` as its host verbatim.
/// A name goes as it is: nvdebug resolves it and names no family for it.
///
/// The text holds the password, so it comes back secret and is wiped on
/// drop. Copies the allocator makes while the text grows are not wiped.
fn render_dut_config(targets: &[Target]) -> SecretString {
    let duts: BTreeMap<&str, DutEntry<'_>> = targets
        .iter()
        .map(|t| {
            let entry = DutEntry {
                baseboard: &t.baseboard,
                bmc_ip: t.bmc.host().to_string(),
                bmc_user: t.bmc.user(),
                bmc_pass: t.bmc.password().expose_secret(),
                ip_network: matches!(t.bmc.host(), BmcHost::Ip(ip) if ip.is_ipv6())
                    .then_some("ipv6"),
            };
            (t.name.as_str(), entry)
        })
        .collect();

    // SAFETY: the keys are strings and every value is a string or a plain
    // struct of strings, which serde_json always serializes.
    let text = serde_json::to_string(&duts).expect("a map of strings always serializes");
    SecretString::from(text)
}

/// The argument vector after the executable, in nvdebug's own flag names.
fn collect_args(dut_config: &Path, request: &CollectRequest, dry_run: bool) -> Vec<OsString> {
    let ids = request
        .collectors
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(" ");

    let mut args: Vec<OsString> = vec![
        "collect".into(),
        "--dut-config".into(),
        dut_config.into(),
        "--collector-id".into(),
        ids.into(),
        "--output".into(),
        request.output_dir.as_os_str().into(),
        "--skip-html-reports".into(),
        "--skip-zip".into(),
        "--non-interactive".into(),
    ];

    if dry_run {
        args.push("--dry-run".into());
    }

    args
}
