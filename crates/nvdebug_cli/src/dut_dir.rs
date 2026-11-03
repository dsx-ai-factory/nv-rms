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
//! The private directory that holds one run's DUT file.

use std::ffi::OsStr;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use crate::Error;

/// Start of a DUT directory's name, followed by the creating process's id,
/// a `-`, and a random suffix.
const DUT_DIR_PREFIX: &str = "nvdebug-";

/// A directory `nvdebug-<pid>-<random>`, mode 0700, under a temp root. Every
/// filesystem call it makes runs on the blocking pool, so a stalled mount
/// holds a pool thread, never an executor thread.
///
/// [`DutDir::remove`] removes it and reports how that went. A `DutDir`
/// dropped without it spawns its removal on the runtime and returns at once,
/// and logs a failure, since nothing else hears of it. Outside a runtime there
/// is no executor to stall, and it removes the directory on the spot, with a
/// warning.
#[derive(Debug)]
pub(crate) struct DutDir {
    /// `None` once [`DutDir::remove`] has taken it, so drop has nothing left
    /// to do.
    path: Option<PathBuf>,
}

impl DutDir {
    /// Creates the directory under `root`, mode 0700 from the start, so it is
    /// never listable on a 022 host. The process id in the name lets the
    /// start-up sweep tell this process's directories from another's and a
    /// stale one from a live one.
    pub(crate) async fn create(root: &Path) -> Result<Self, Error> {
        let parent = root.to_path_buf();

        let out = tokio::task::spawn_blocking(move || {
            tempfile::Builder::new()
                .prefix(&format!("{DUT_DIR_PREFIX}{}-", std::process::id()))
                .permissions(std::fs::Permissions::from_mode(0o700))
                .tempdir_in(parent)
        })
        .await
        // outer err from tokio::spawn_blocking: the task panicked or the
        // runtime dropped it, a bug or a dying process
        .map_err(Error::failed("could not create DUT directory"))?;

        // inner err from tempfile: the temp root itself, for an operator.
        // The TempDir is kept only here. A creation whose future was dropped
        // leaves its TempDir to nobody, and that drop removes the directory.
        let p = out
            .map_err(Error::failed("could not create DUT directory"))?
            .keep();

        Ok(Self { path: Some(p) })
    }

    pub(crate) fn path(&self) -> &Path {
        self.path
            .as_deref()
            // SAFETY: create() stores a path. remove() consumes self before
            // taking it, and drop() takes it only during destruction.
            .expect("invariant violated: path can't be None")
    }

    /// Removes the directory and everything in it. A directory already gone
    /// counts as removed. A failure is [`Error::Failed`] naming the
    /// directory and the cause.
    pub(crate) async fn remove(mut self) -> Result<(), Error> {
        match self.path.take() {
            Some(path) => remove_dir(path).await,
            None => Ok(()),
        }
    }
}

impl Drop for DutDir {
    fn drop(&mut self) {
        let Some(path) = self.path.take() else {
            return;
        };

        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move {
                    if let Err(error) = remove_dir(path).await {
                        tracing::warn!(
                            error = &error as &dyn std::error::Error,
                            "the DUT directory is left for the start-up sweep"
                        );
                    }
                });
            }
            Err(_) => {
                tracing::warn!(path = %path.display(), "no tokio runtime: removing the DUT directory synchronously");
                if let Err(error) = removed(&path, std::fs::remove_dir_all(&path)) {
                    tracing::warn!(
                        error = &error as &dyn std::error::Error,
                        "the DUT directory is left for the start-up sweep"
                    );
                }
            }
        }
    }
}

/// The removal [`DutDir::remove`] and drop share.
async fn remove_dir(path: PathBuf) -> Result<(), Error> {
    removed(&path, tokio::fs::remove_dir_all(&path).await)
}

/// A removal's outcome. A directory already gone counts as removed.
fn removed(dir: &Path, result: std::io::Result<()>) -> Result<(), Error> {
    match result {
        Err(cause) if cause.kind() == std::io::ErrorKind::NotFound => Ok(()),
        result => result.map_err(Error::failed_path("remove failed", dir)),
    }
}

/// Removes DUT directories left in the temp root by processes that no
/// longer run. A process killed with SIGKILL cannot remove its own, and the
/// DUT file inside holds a BMC password. Call it once when the process
/// starts, before it runs nvdebug. The process id in a directory's name
/// says whose it is. A directory whose process still runs is kept. One with
/// this process's id is removed: this process has made none yet, so an
/// earlier process with the same id left it, as PID 1 does across restarts
/// of a container whose temp root survives. Another reused id keeps a stale
/// directory until the next start.
///
/// Whether a process runs is read from `/proc`. Outside Linux there is no
/// `/proc`, every process counts as running, and nothing is removed.
pub async fn sweep_stale_dut_dirs() {
    sweep_dut_dirs(&std::env::temp_dir(), is_running).await;
}

/// The sweep over `root`, with the liveness check passed in.
async fn sweep_dut_dirs(root: &Path, is_running: impl Fn(u32) -> bool) {
    let stale = match stale_dut_dirs(root, is_running).await {
        Ok(stale) => stale,
        Err(error) => {
            tracing::warn!(root = %root.display(), %error, "could not list the temp root for stale DUT directories");
            return;
        }
    };

    for (path, pid) in stale {
        match tokio::fs::remove_dir_all(&path).await {
            Ok(()) => {
                tracing::info!(path = %path.display(), creator_pid = pid, "removed a stale DUT directory");
            }
            Err(error) => {
                tracing::warn!(path = %path.display(), creator_pid = pid, %error, "could not remove a stale DUT directory");
            }
        }
    }
}

/// The DUT directories under `root` whose process no longer runs, with the
/// process id from each name.
async fn stale_dut_dirs(
    root: &Path,
    is_running: impl Fn(u32) -> bool,
) -> std::io::Result<Vec<(PathBuf, u32)>> {
    let mut entries = tokio::fs::read_dir(root).await?;
    let mut stale = Vec::new();

    while let Some(entry) = entries.next_entry().await? {
        let Some(pid) = dut_dir_pid(&entry.file_name()) else {
            continue;
        };

        // `file_type` does not follow a link, so a link with a DUT
        // directory's name is skipped, never followed into its target.
        let is_dir = entry.file_type().await.is_ok_and(|kind| kind.is_dir());
        if is_dir && (pid == std::process::id() || !is_running(pid)) {
            stale.push((entry.path(), pid));
        }
    }

    Ok(stale)
}

/// The creating process's id, when `name` is a DUT directory's name.
/// nvdebug's own temporary directories also start with `nvdebug-`, so the
/// id must be all digits.
fn dut_dir_pid(name: &OsStr) -> Option<u32> {
    let rest = name.to_str()?.strip_prefix(DUT_DIR_PREFIX)?;
    let (pid, suffix) = rest.split_once('-')?;
    if pid.is_empty() || !pid.bytes().all(|b| b.is_ascii_digit()) || suffix.is_empty() {
        return None;
    }

    pid.parse().ok()
}

/// Whether a process with this id runs, read from `/proc`.
fn is_running(pid: u32) -> bool {
    !cfg!(target_os = "linux") || Path::new("/proc").join(pid.to_string()).exists()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn a_creation_whose_future_is_dropped_leaves_no_directory() {
        let root = tempfile::tempdir().expect("root");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .expect("runtime");

        runtime.block_on(async {
            // Hold the one blocking thread, so the creation queues behind it.
            let (release, held) = std::sync::mpsc::channel::<()>();
            let blocker = tokio::task::spawn_blocking(move || held.recv());
            {
                let create = DutDir::create(root.path());
                tokio::pin!(create);
                // One poll queues the creation; then its future is dropped.
                assert!(
                    tokio::time::timeout(Duration::ZERO, &mut create)
                        .await
                        .is_err()
                );
            }
            release.send(()).expect("released");
            blocker.await.expect("joined").expect("received");
            // Queued after the creation on the one thread, so the creation
            // has run, and its result has been dropped, once this returns.
            tokio::task::spawn_blocking(|| ()).await.expect("joined");
        });

        let left = std::fs::read_dir(root.path()).expect("listed").count();
        assert_eq!(left, 0);
    }

    #[tokio::test]
    async fn sweep_removes_only_dut_directories_of_processes_not_running() {
        const RUNNING: u32 = 4242;
        const GONE: u32 = 4243;
        let dir = tempfile::tempdir().expect("root");
        // On macOS the temp root itself sits behind a link.
        let root = dir.path().canonicalize().expect("resolved");
        let outside = root.join("outside");
        std::fs::create_dir(&outside).expect("created");
        std::fs::write(outside.join("keep"), b"").expect("written");
        let own = std::process::id();

        let dirs = [
            (format!("nvdebug-{GONE}-a1b2c3"), false),
            (format!("nvdebug-{RUNNING}-a1b2c3"), true),
            // An earlier process with this id left it, as PID 1 does.
            (format!("nvdebug-{own}-a1b2c3"), false),
            ("nvdebug-7z-helper-a1b2c3".to_owned(), true),
            (format!("nvdebug-+{GONE}-a1b2c3"), true),
            (format!("nvdebug-{GONE}-"), true),
        ];
        for (name, _) in &dirs {
            std::fs::create_dir(root.join(name)).expect("created");
            std::fs::write(root.join(name).join("dut_config.json"), b"{}").expect("written");
        }
        let file = root.join(format!("nvdebug-{GONE}-file01"));
        std::fs::write(&file, b"").expect("written");
        let link = root.join(format!("nvdebug-{GONE}-link01"));
        std::os::unix::fs::symlink(&outside, &link).expect("linked");

        sweep_dut_dirs(&root, |pid| pid == RUNNING || pid == own).await;

        for (name, kept) in &dirs {
            assert_eq!(root.join(name).exists(), *kept, "{name}");
        }
        assert!(file.exists());
        assert!(link.symlink_metadata().is_ok());
        assert!(outside.join("keep").exists());
    }

    #[test]
    fn a_dir_dropped_outside_a_runtime_is_removed_on_the_spot() {
        let root = tempfile::tempdir().expect("root");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        let dir = runtime
            .block_on(DutDir::create(root.path()))
            .expect("created");
        let path = dir.path().to_path_buf();

        drop(dir);

        assert!(!path.exists());
    }
}
