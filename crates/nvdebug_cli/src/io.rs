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
//! The async shell: check, spawn, wait, and clean up.

use std::error::Error as StdError;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::{Duration, SystemTime};

use nix::errno::Errno;
use nix::sys::signal::{Signal, killpg};
use nix::unistd::Pid;
use secrecy::ExposeSecret;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};
use tokio_util::sync::CancellationToken;

use crate::dut_dir::DutDir;
use crate::{CollectRequest, Collection, Error, Executable, collect_args, render_dut_config};

/// How long nvdebug's process group gets after SIGTERM before SIGKILL.
/// nvdebug traps SIGTERM: it cancels its collectors and kills its helpers,
/// which a SIGKILL of nvdebug would leave running.
const GRACE_AFTER_TERM: Duration = Duration::from_secs(10);

/// How often a stopping run asks whether its process group is empty.
const GROUP_POLL: Duration = Duration::from_millis(50);

/// The start of the name nvdebug gives a run's directory under the output
/// directory: `nvdebug_logs_<local time>`, to the second.
const RUN_DIR_PREFIX: &str = "nvdebug_logs_";

/// The caller's environment variables nvdebug and its helpers get, besides
/// every `LC_*`: where to find ipmitool and ssh, ssh's home, where the
/// one-file build unpacks, text encoding, the local time that names run
/// directories, and a trust store a deployment may point elsewhere. The
/// rest stays out: the caller's secrets, and nvdebug's own `NVDEBUG_*` and
/// `BASEBOARD`, which would override what this crate passes.
const KEPT_ENV: [&str; 8] = [
    "PATH",
    "HOME",
    "TMPDIR",
    "LANG",
    "TZ",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "REQUESTS_CA_BUNDLE",
];

/// Name of the DUT file inside its private directory.
const DUT_CONFIG_FILE: &str = "dut_config.json";

// Error helpers for the failures only this module finds: the executable
// unusable, the output directory refused. They live next to their call
// sites.

/// For map_err() calls on the executable: `what` failed on `path`, and the
/// error is kept as the cause.
fn unavailable_path<E>(what: &'static str, path: &Path) -> impl FnOnce(E) -> Error
where
    E: StdError + Send + Sync + 'static,
{
    move |cause| Error::unavailable(format!("{what} {}", path.display()))(cause)
}

/// `--version` did not succeed. nvdebug's own output is logged at the
/// failure.
fn version_failed(path: &Path, status: ExitStatus) -> Error {
    Error::new_unavailable(format!("version failed {}: {status}", path.display()))
}

/// A FIFO, a device or a directory where the executable should be.
fn not_a_file(path: &Path) -> Error {
    Error::new_unavailable(format!(
        "lookup failed {}: not a regular file",
        path.display()
    ))
}

/// The output directory is refused, for the reason `why` gives.
fn invalid_output_dir(why: &str) -> Error {
    Error::new_invalid("output directory", why)
}

impl Executable {
    /// Runs `nvdebug --version` and returns its first line. A failure here is
    /// how RMS marks log collection unavailable. Unlike a collection, this
    /// call reads nvdebug's output: the line is the answer. `--version`
    /// starts no helpers, so a plain capture of both pipes cannot hang on
    /// one, and nvdebug prints a line or two here.
    pub async fn version(
        &self,
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<String, Error> {
        let asked = async {
            self.check_file().await?;
            // output() pipes stdout and stderr itself.
            self.command(&["--version".into()])
                .output()
                .await
                .map_err(unavailable_path("spawn failed", &self.path))
        };
        let asked = tokio::time::timeout(timeout, asked);
        let output = tokio::select! {
            output = asked => output.map_err(|_| Error::Timeout(timeout))??,
            () = cancel.cancelled() => return Err(Error::Cancelled),
        };

        if !output.status.success() {
            tracing::warn!(
                status = %output.status,
                stdout = %text(&output.stdout),
                stderr = %text(&output.stderr),
                "nvdebug --version failed"
            );
            return Err(version_failed(&self.path, output.status));
        }

        Ok(String::from_utf8_lossy(&output.stdout)
            .lines()
            .next()
            .unwrap_or_default()
            .trim()
            .to_owned())
    }

    /// Validates the request on nvdebug's side without contacting hardware.
    pub async fn dry_run(
        &self,
        request: &CollectRequest,
        cancel: &CancellationToken,
    ) -> Result<Collection, Error> {
        self.run_collect(request, true, &std::env::temp_dir(), cancel)
            .await
    }

    /// Collects from the request's targets into its output directory.
    pub async fn collect(
        &self,
        request: &CollectRequest,
        cancel: &CancellationToken,
    ) -> Result<Collection, Error> {
        self.run_collect(request, false, &std::env::temp_dir(), cancel)
            .await
    }

    /// One collection, dry or real: write the DUT file under `temp_root`,
    /// run, remove the file, hand back what nvdebug left.
    async fn run_collect(
        &self,
        request: &CollectRequest,
        dry_run: bool,
        temp_root: &Path,
        cancel: &CancellationToken,
    ) -> Result<Collection, Error> {
        tracing::info!(
            targets = request.targets.len(),
            collectors = %request.collectors.iter().map(ToString::to_string).collect::<Vec<_>>().join(" "),
            output_dir = %request.output_dir.display(),
            dry_run,
            "collecting with nvdebug"
        );

        // Held until the run returns. A second run on the directory is
        // refused meanwhile, whatever process makes it.
        let _claim = claim_output_dir(&request.output_dir).await?;

        // Every return below removes the directory first, so the password
        // does not outlive the run. A future dropped outright leaves it to
        // `DutDir`'s drop, a best effort.
        let (dut_dir, dut_config) = write_dut_config(temp_root, &request.targets).await?;
        let args = collect_args(&dut_config, request, dry_run);

        let started = SystemTime::now();
        let status = self.run(&args, request.timeout, cancel).await;

        // Removed before either result returns. A timeout or a cancel comes
        // back here as a value, a cancel at RMS's shutdown too, and a
        // detached removal may not run once the runtime ends. A failure to
        // remove is logged, not returned: it is no reason to drop the logs.
        if let Err(error) = dut_dir.remove().await {
            tracing::warn!(
                error = &error as &dyn std::error::Error,
                "the DUT directory is left for the start-up sweep"
            );
        }
        let status = status?;

        tracing::info!(%status, "nvdebug finished");
        Ok(Collection {
            status,
            output_dir: request.output_dir.clone(),
            started,
        })
    }

    /// Checks the file, then runs it with `args` until it exits, the timeout
    /// passes, or `cancel` fires. The timeout and the token cover the check
    /// too, so a slow disk cannot hold the caller past its budget.
    ///
    /// nvdebug leads a process group of its own. Its helpers join it, unless
    /// one starts a session of its own, as nvdebug 2.2.0 does for some. On
    /// timeout or cancel the group gets SIGTERM. nvdebug traps it, cancels its
    /// collectors and kills its helpers, those in sessions of their own
    /// included. The group then gets up to `GRACE_AFTER_TERM` to empty, so the
    /// call can return that much past the timeout. What is left gets SIGKILL,
    /// and so does the whole group when the call's future is dropped. A helper
    /// outside the group is out of reach of either SIGKILL.
    async fn run(
        &self,
        args: &[OsString],
        timeout: Duration,
        cancel: &CancellationToken,
    ) -> Result<ExitStatus, Error> {
        tracing::debug!(path = %self.path.display(), ?args, "running nvdebug");

        let stop = async {
            tokio::select! {
                () = tokio::time::sleep(timeout) => Error::Timeout(timeout),
                () = cancel.cancelled() => Error::Cancelled,
            }
        };
        // One stop for the whole run: the start races it, then the wait
        // races the same future, so the timeout counts from here. select!
        // polls it through `&mut`, which needs it pinned.
        tokio::pin!(stop);

        let (mut child, group) = tokio::select! {
            started = self.start(args) => started?,
            error = &mut stop => return Err(error),
        };

        let wait_failed = Error::failed("could not wait for nvdebug");
        let error = tokio::select! {
            status = child.wait() => return status.map_err(wait_failed),
            error = &mut stop => error,
        };
        stop_group(&mut child, &group).await;
        Err(error)
    }

    /// The command both calls start from: the file, `args`, only the kept
    /// part of the caller's environment, no stdin, and a kill when the child
    /// handle is dropped.
    fn command(&self, args: &[OsString]) -> Command {
        let mut command = Command::new(&self.path);
        command
            .args(args)
            .env_clear()
            .envs(kept_env())
            .stdin(Stdio::null())
            .kill_on_drop(true);
        command
    }

    /// Checks the file and starts it as the leader of a new process group,
    /// its output discarded.
    async fn start(&self, args: &[OsString]) -> Result<(Child, ProcessGroup), Error> {
        self.check_file().await?;

        // TODO: nothing checks that this is the executable RMS was configured
        // with, which RFD 0001 asks for: hash-verified or marked unavailable.
        // A digest of a file's content belongs in a utility crate, not here.
        // The options:
        // - hash once, where the configuration is parsed. A swap after start
        //   goes unseen until a restart. Enough when nvdebug cannot change
        //   for the process's life: in the image, or on a read-only mount.
        // - hash again before each spawn. A swap after start is caught, and
        //   the window shrinks to the time between the hash and the exec.
        // Neither closes the window. Closing it means executing the hashed
        // descriptor itself (fexecve, or /proc/self/fd), untried against the
        // one-file build.
        //
        // TODO: discuss what RMS keeps of nvdebug's own output. Its stdout and
        // stderr go to /dev/null here: nothing reads them, so nothing can
        // block on a full pipe or on a helper that holds one open. What that
        // gives up, to discuss:
        // - Is nvdebug's own log enough? It writes one into the run directory
        //   (`nvdebug_runtime_output.txt`), and a broken install already shows
        //   at `--version`, which reads the output. A crash after startup and
        //   before nvdebug opens its log leaves no trace at all.
        // - Does RMS care about a crash as opposed to a failed collection? Both
        //   can exit 1. What tells them apart is nvdebug's run result
        //   (`.metadata/run_result.json`), which a crash may not write.
        // For now RMS only needs to know that nvdebug did not start. This
        // crate is temporary: nvdebug's Rust crate replaces it.
        let mut command = self.command(args);
        command
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);
        let child = command
            .spawn()
            .map_err(unavailable_path("spawn failed", &self.path))?;
        let group = ProcessGroup::of(&child);

        Ok((child, group))
    }

    /// Checks that the path is a regular file. A FIFO or a device would
    /// block the spawn's read for as long as nothing writes to it, past any
    /// budget. Only a regular file can be nvdebug.
    async fn check_file(&self) -> Result<(), Error> {
        // This is where the path is read, so this is where it is logged.
        tracing::debug!(path = %self.path.display(), "checking nvdebug");

        let metadata = tokio::fs::metadata(&self.path)
            .await
            .map_err(unavailable_path("lookup failed", &self.path))?;
        if !metadata.is_file() {
            return Err(not_a_file(&self.path));
        }

        Ok(())
    }
}

/// Writes the DUT file, mode 0600, into a fresh [`DutDir`] under `root`, the
/// system temp root in practice. It honours `TMPDIR`, so the deployment
/// decides whether the password touches a disk or a memory mount. A failure
/// after the directory exists drops it, which removes it.
async fn write_dut_config(
    root: &Path,
    targets: &[crate::Target],
) -> Result<(DutDir, PathBuf), Error> {
    let dir = DutDir::create(root).await?;

    let path = dir.path().join(DUT_CONFIG_FILE);
    let written = write_private(&path, render_dut_config(targets).expose_secret().as_bytes())
        .await
        .map_err(Error::failed_path("write failed", &path));
    if let Err(failure) = written {
        // Removed before the error returns, as run_collect does after a run.
        if let Err(error) = dir.remove().await {
            tracing::warn!(
                error = &error as &dyn std::error::Error,
                "the DUT directory is left for the start-up sweep"
            );
        }
        return Err(failure);
    }

    Ok((dir, path))
}

/// Creates `path` with mode 0600 and writes `bytes` to it. Fails if the file
/// exists.
async fn write_private(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .await?;
    file.write_all(bytes).await?;
    file.flush().await
}

/// The part of this process's environment on `KEPT_ENV`, plus every `LC_*`.
fn kept_env() -> impl Iterator<Item = (OsString, OsString)> {
    std::env::vars_os().filter(|(name, _)| {
        name.to_str()
            .is_some_and(|name| KEPT_ENV.contains(&name) || name.starts_with("LC_"))
    })
}

/// Claims the output directory for one run: creates it if missing, takes
/// an exclusive lock on it, and refuses it if it already holds an nvdebug
/// run. Two runs in one directory could share a name to the second, and a
/// caller could not tell which is its own.
///
/// The lock is a `flock` on the directory itself, so nothing is left in
/// the output, and the kernel releases it when the returned file closes or
/// the process dies. It binds only callers that take it: nvdebug does not.
/// The check runs under the lock, so two concurrent runs cannot both pass it.
/// A caller killed outright drops the lock while its nvdebug may still run,
/// and a later run can then share the directory with that nvdebug.
async fn claim_output_dir(output_dir: &Path) -> Result<std::fs::File, Error> {
    let output_dir = output_dir.to_owned();
    let claim = tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&output_dir)?;
        let dir = std::fs::File::open(&output_dir)?;
        match dir.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Ok(Err("another run holds it")),
            Err(std::fs::TryLockError::Error(error)) => return Err(error),
        }

        for entry in std::fs::read_dir(&output_dir)? {
            let is_run = entry?
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(RUN_DIR_PREFIX));
            if is_run {
                return Ok(Err("already holds an nvdebug run"));
            }
        }
        std::io::Result::Ok(Ok(dir))
    });

    claim
        .await
        .map_err(Error::failed("could not claim output directory"))?
        .map_err(Error::failed("could not claim output directory"))?
        .map_err(invalid_output_dir)
}

/// nvdebug's process group: nvdebug leads it, and its helpers join it,
/// except those that start a session of their own. Dropping it sends
/// SIGKILL to whatever of the group still runs.
struct ProcessGroup(Pid);

impl ProcessGroup {
    fn of(child: &Child) -> Self {
        let pid = child
            .id()
            // SAFETY: tokio holds a child's id until the child has been
            // waited for, and this one was spawned just now, never waited on.
            .expect("invariant violated: a child just spawned has an id");
        Self(Pid::from_raw(pid as i32))
    }

    /// Asks the group to stop. nvdebug traps SIGTERM.
    fn terminate(&self) {
        signal_group(self.0, Signal::SIGTERM);
    }

    /// Resolves once no process of the group is left. Nothing waits on a
    /// group, so this asks every `GROUP_POLL`. A zombie still counts: where
    /// no one reaps orphans, this lasts until its caller's bound.
    async fn emptied(&self) {
        while killpg(self.0, None).is_ok() {
            tokio::time::sleep(GROUP_POLL).await;
        }
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        signal_group(self.0, Signal::SIGKILL);
    }
}

/// A group that is gone has nothing left to stop, so ESRCH is not logged.
fn signal_group(group: Pid, signal: Signal) {
    match killpg(group, signal) {
        Ok(()) | Err(Errno::ESRCH) => {}
        Err(error) => {
            tracing::warn!(%error, ?signal, "could not signal nvdebug's process group");
        }
    }
}

/// SIGTERM to the group, then up to `GRACE_AFTER_TERM` for nvdebug to exit
/// and the group to empty. The grace is the whole group's, not only
/// nvdebug's: a helper can still be closing its BMC session after nvdebug
/// exits. What is left gets SIGKILL when the group is dropped.
async fn stop_group(child: &mut Child, group: &ProcessGroup) {
    group.terminate();
    let emptied = async {
        child.wait().await.ok();
        group.emptied().await;
    };
    tokio::time::timeout(GRACE_AFTER_TERM, emptied).await.ok();
}

/// Bytes as text, trimmed.
fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).trim().to_owned()
}
