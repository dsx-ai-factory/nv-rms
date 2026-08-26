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

//! Password-based SSH/SFTP transport helpers.
//!
//! Provides async SSH command execution and SFTP upload helpers built on
//! `russh` / `russh-sftp`.

use std::path::PathBuf;
#[cfg(test)]
use std::sync::LazyLock;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use russh::keys::{known_hosts, PublicKey, PublicKeyOrCertificate};
use russh::{client, ChannelMsg, Disconnect};
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::{FileAttributes, OpenFlags};
use tokio::io::AsyncWriteExt;

const SFTP_BUF_SIZE: usize = 128 * 1024;
const DEFAULT_KNOWN_HOSTS_RELATIVE_PATH: &str = ".ssh/known_hosts";

/// Host-key verification behavior for NVFWUPD SSH/SFTP sessions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SshHostKeyMode {
    /// Learn the first key for a host, then reject changed keys.
    TrustOnFirstUse,
    /// Require the host key to already be present in known_hosts.
    Strict,
    /// Accept any host key. This is only for explicit insecure opt-out.
    InsecureAcceptAny,
}

/// Host-key verification policy and optional known_hosts location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SshHostKeyPolicy {
    mode: SshHostKeyMode,
    known_hosts_path: Option<PathBuf>,
}

impl SshHostKeyPolicy {
    pub(crate) fn trust_on_first_use(known_hosts_path: Option<String>) -> Self {
        Self {
            mode: SshHostKeyMode::TrustOnFirstUse,
            known_hosts_path: known_hosts_path.map(PathBuf::from),
        }
    }

    pub(crate) fn strict(known_hosts_path: Option<String>) -> Self {
        Self {
            mode: SshHostKeyMode::Strict,
            known_hosts_path: known_hosts_path.map(PathBuf::from),
        }
    }

    pub(crate) fn insecure_accept_any() -> Self {
        Self {
            mode: SshHostKeyMode::InsecureAcceptAny,
            known_hosts_path: None,
        }
    }

    fn verify(&self, host: &str, port: u16, key: &PublicKey) -> Result<(), String> {
        if self.mode == SshHostKeyMode::InsecureAcceptAny {
            tracing::warn!(
                "SSH host-key verification is disabled for {host}:{port}; accepting presented host key without validation"
            );
            return Ok(());
        }

        let path = self.resolve_known_hosts_path()?;
        let known = known_hosts::known_host_keys_path(host, port, &path).map_err(|e| {
            format!(
                "SSH host key verification failed for {host}:{port}: \
                 failed to read known_hosts file {}: {e}",
                path.display()
            )
        })?;

        if known
            .iter()
            .any(|(_, known_key)| known_key.key_data() == key.key_data())
        {
            return Ok(());
        }

        let fingerprint = key.fingerprint(Default::default()).to_string();
        let algorithm = key.algorithm().as_str().to_string();
        if known.is_empty() && self.mode == SshHostKeyMode::TrustOnFirstUse {
            Self::create_known_hosts_parent_dir(&path)?;
            known_hosts::learn_known_hosts_path(host, port, key, &path).map_err(|e| {
                format!(
                    "SSH host key verification failed for {host}:{port}: \
                     failed to write known_hosts file {}: {e}",
                    path.display()
                )
            })?;
            tracing::info!(
                "Learned SSH host key for {host}:{port} in {} ({algorithm} {fingerprint})",
                path.display()
            );
            return Ok(());
        }

        if known.is_empty() {
            return Err(format!(
                "SSH host key verification failed for {host}:{port}: \
                 no matching key found in {}. Use ssh_known_hosts=<path> with a trusted \
                 known_hosts entry, use ssh_host_key_mode=tofu to learn the first key, \
                 or pass ssh_host_key_mode=disabled only when host-key validation must be disabled. \
                 Presented key: {algorithm} {fingerprint}",
                path.display()
            ));
        }

        let expected = known
            .iter()
            .map(|(_, known_key)| {
                format!(
                    "{} {}",
                    known_key.algorithm().as_str(),
                    known_key.fingerprint(Default::default())
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        Err(format!(
            "SSH host key verification failed for {host}:{port}: \
             presented key does not match known_hosts file {}. \
             Expected one of: {expected}. Presented key: {algorithm} {fingerprint}. \
             If the host key intentionally changed, update the known_hosts entry.",
            path.display()
        ))
    }

    fn resolve_known_hosts_path(&self) -> Result<PathBuf, String> {
        if let Some(path) = &self.known_hosts_path {
            return Ok(path.clone());
        }

        let home = std::env::var_os("HOME").ok_or_else(|| {
            "SSH host key verification requires ssh_known_hosts=<path> because HOME is not set"
                .to_string()
        })?;
        Ok(PathBuf::from(home).join(DEFAULT_KNOWN_HOSTS_RELATIVE_PATH))
    }

    fn create_known_hosts_parent_dir(path: &PathBuf) -> Result<(), String> {
        let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        else {
            return Ok(());
        };

        std::fs::create_dir_all(parent).map_err(|e| {
            format!(
                "SSH host key verification failed: failed to create known_hosts directory {}: {e}",
                parent.display()
            )
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SshExecOutput {
    pub(crate) success: bool,
    pub(crate) stdout: String,
    pub(crate) stderr: String,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MockExecCall {
    pub(crate) command: String,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MockUploadCall {
    pub(crate) local_path: PathBuf,
    pub(crate) remote_path: String,
}

#[cfg(test)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MockSshSnapshot {
    pub(crate) exec_calls: Vec<MockExecCall>,
    pub(crate) upload_calls: Vec<MockUploadCall>,
}

#[cfg(test)]
#[derive(Debug, Clone, Default)]
struct MockSshState {
    exec_calls: Vec<MockExecCall>,
    upload_calls: Vec<MockUploadCall>,
}

#[cfg(test)]
static MOCK_SSH: LazyLock<Mutex<std::collections::HashMap<String, MockSshState>>> =
    LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));

#[cfg(test)]
pub(crate) fn install_mock_for_host(host: &str) {
    MOCK_SSH
        .lock()
        .expect("mock ssh mutex poisoned")
        .insert(host.to_string(), MockSshState::default());
}

#[cfg(test)]
pub(crate) fn take_mock_snapshot(host: &str) -> Option<MockSshSnapshot> {
    MOCK_SSH
        .lock()
        .expect("mock ssh mutex poisoned")
        .remove(host)
        .map(|state| MockSshSnapshot {
            exec_calls: state.exec_calls,
            upload_calls: state.upload_calls,
        })
}

#[cfg(test)]
fn mock_exec_result(
    host: &str,
    command: &str,
    _stdin: Option<String>,
) -> Option<Result<SshExecOutput, String>> {
    let mut mocks = MOCK_SSH.lock().expect("mock ssh mutex poisoned");
    let state = mocks.get_mut(host)?;
    state.exec_calls.push(MockExecCall {
        command: command.to_string(),
    });
    Some(Ok(SshExecOutput {
        success: true,
        stdout: String::new(),
        stderr: String::new(),
    }))
}

#[cfg(test)]
fn mock_upload_result(
    host: &str,
    local_path: &PathBuf,
    remote_path: &str,
) -> Option<Result<(), String>> {
    let mut mocks = MOCK_SSH.lock().expect("mock ssh mutex poisoned");
    let state = mocks.get_mut(host)?;
    state.upload_calls.push(MockUploadCall {
        local_path: local_path.clone(),
        remote_path: remote_path.to_string(),
    });
    Some(Ok(()))
}

#[cfg(test)]
fn mock_upload_with_setup_result(
    host: &str,
    setup_commands: &[String],
    local_path: &PathBuf,
    remote_path: &str,
) -> Option<Result<(), String>> {
    let mut mocks = MOCK_SSH.lock().expect("mock ssh mutex poisoned");
    let state = mocks.get_mut(host)?;
    for command in setup_commands {
        state.exec_calls.push(MockExecCall {
            command: command.clone(),
        });
    }
    state.upload_calls.push(MockUploadCall {
        local_path: local_path.clone(),
        remote_path: remote_path.to_string(),
    });
    Some(Ok(()))
}

struct VerifyServerKey {
    host: String,
    port: u16,
    policy: SshHostKeyPolicy,
    failure: Arc<Mutex<Option<String>>>,
}

impl client::Handler for VerifyServerKey {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        server_public_key: &PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        let public_key = server_public_key.public_key();
        match self.policy.verify(&self.host, self.port, &public_key) {
            Ok(()) => Ok(true),
            Err(message) => {
                if let Ok(mut failure) = self.failure.lock() {
                    *failure = Some(message);
                }
                Ok(false)
            }
        }
    }
}

pub(crate) async fn execute_command_async(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    timeout_secs: u64,
    command: &str,
    stdin: Option<String>,
    host_key_policy: SshHostKeyPolicy,
) -> Result<SshExecOutput, String> {
    #[cfg(test)]
    if let Some(result) = mock_exec_result(host, command, stdin.clone()) {
        return result;
    }

    let mut session =
        connect_password(host, port, user, password, timeout_secs, host_key_policy).await?;
    let result = tokio::time::timeout(
        Duration::from_secs(timeout_secs),
        exec_on_session(&mut session, command, stdin),
    )
    .await
    .map_err(|_| format!("SSH command timed out after {timeout_secs}s"))
    .and_then(|result| result);
    let _ = session
        .disconnect(Disconnect::ByApplication, "", "English")
        .await;
    result
}

async fn connect_password(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    timeout_secs: u64,
    host_key_policy: SshHostKeyPolicy,
) -> Result<client::Handle<VerifyServerKey>, String> {
    let config = client::Config {
        inactivity_timeout: Some(Duration::from_secs(timeout_secs)),
        ..Default::default()
    };
    let key_failure = Arc::new(Mutex::new(None));
    let connect = client::connect(
        Arc::new(config),
        (host, port),
        VerifyServerKey {
            host: host.to_string(),
            port,
            policy: host_key_policy,
            failure: key_failure.clone(),
        },
    );
    let mut session = tokio::time::timeout(Duration::from_secs(timeout_secs), connect)
        .await
        .map_err(|_| format!("SSH connect timed out after {timeout_secs}s"))?
        .map_err(|e| {
            if let Ok(mut failure) = key_failure.lock() {
                if let Some(message) = failure.take() {
                    return format!("{message}; details: {e}");
                }
            }
            format!("SSH connect failed: {e}")
        })?;

    let authenticated = tokio::time::timeout(
        Duration::from_secs(timeout_secs),
        session.authenticate_password(user, password),
    )
    .await
    .map_err(|_| format!("SSH auth timed out after {timeout_secs}s"))?
    .map_err(|e| format!("SSH auth failed: {e}"))?;

    if !authenticated.success() {
        return Err("SSH authentication failed".to_string());
    }

    Ok(session)
}

async fn exec_on_session(
    session: &mut client::Handle<VerifyServerKey>,
    command: &str,
    stdin: Option<String>,
) -> Result<SshExecOutput, String> {
    let mut channel = session
        .channel_open_session()
        .await
        .map_err(|e| format!("Channel open failed: {e}"))?;

    channel
        .exec(true, command)
        .await
        .map_err(|e| format!("Exec failed: {e}"))?;

    if let Some(stdin) = stdin {
        {
            let mut writer = channel.make_writer();
            writer
                .write_all(stdin.as_bytes())
                .await
                .map_err(|e| format!("SSH stdin write failed: {e}"))?;
            writer
                .shutdown()
                .await
                .map_err(|e| format!("SSH stdin close failed: {e}"))?;
        }
        let _ = channel.eof().await;
    }

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut exit_status = None;

    while let Some(msg) = channel.wait().await {
        match msg {
            ChannelMsg::Data { data } => stdout.extend_from_slice(&data),
            ChannelMsg::ExtendedData { data, .. } => stderr.extend_from_slice(&data),
            ChannelMsg::ExitStatus { exit_status: code } => exit_status = Some(code),
            _ => {}
        }
    }

    let code = exit_status.unwrap_or(u32::MAX);
    Ok(SshExecOutput {
        success: code == 0,
        stdout: String::from_utf8_lossy(&stdout).into_owned(),
        stderr: String::from_utf8_lossy(&stderr).into_owned(),
    })
}

pub(crate) async fn upload_file_async(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    timeout_secs: u64,
    local_path: PathBuf,
    remote_path: String,
    host_key_policy: SshHostKeyPolicy,
) -> Result<(), String> {
    #[cfg(test)]
    if let Some(result) = mock_upload_result(host, &local_path, &remote_path) {
        return result;
    }

    let local_file = tokio::fs::File::open(&local_path)
        .await
        .map_err(|e| format!("Failed to open file {}: {e}", local_path.display()))?;
    let mut reader = tokio::io::BufReader::with_capacity(SFTP_BUF_SIZE, local_file);

    let mut session =
        connect_password(host, port, user, password, timeout_secs, host_key_policy).await?;
    let result = tokio::time::timeout(Duration::from_secs(timeout_secs), async {
        let channel = session
            .channel_open_session()
            .await
            .map_err(|e| format!("SFTP channel open failed: {e}"))?;
        channel
            .request_subsystem(true, "sftp")
            .await
            .map_err(|e| format!("SFTP subsystem request failed: {e}"))?;

        let sftp = SftpSession::new(channel.into_stream())
            .await
            .map_err(|e| format!("SFTP session creation failed: {e}"))?;
        sftp.set_timeout(timeout_secs).await;

        let mut attrs = FileAttributes::empty();
        attrs.permissions = Some(0o644);
        let mut remote_file = sftp
            .open_with_flags_and_attributes(
                remote_path,
                OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE,
                attrs,
            )
            .await
            .map_err(|e| format!("SFTP open failed: {e}"))?;

        tokio::io::copy_buf(&mut reader, &mut remote_file)
            .await
            .map_err(|e| format!("SFTP write failed: {e}"))?;
        remote_file
            .flush()
            .await
            .map_err(|e| format!("SFTP flush failed: {e}"))?;
        remote_file
            .shutdown()
            .await
            .map_err(|e| format!("SFTP close failed: {e}"))?;

        let _ = sftp.close().await;
        Ok(())
    })
    .await
    .map_err(|_| format!("SFTP upload timed out after {timeout_secs}s"))
    .and_then(|result| result);
    let _ = session
        .disconnect(Disconnect::ByApplication, "", "English")
        .await;
    result
}

pub(crate) async fn upload_file_with_setup_async(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    connect_timeout_secs: u64,
    operation_timeout_secs: u64,
    setup_commands: &[String],
    local_path: PathBuf,
    remote_path: String,
    host_key_policy: SshHostKeyPolicy,
) -> Result<(), String> {
    #[cfg(test)]
    if let Some(result) =
        mock_upload_with_setup_result(host, setup_commands, &local_path, &remote_path)
    {
        return result;
    }

    let mut session = connect_password(
        host,
        port,
        user,
        password,
        connect_timeout_secs,
        host_key_policy,
    )
    .await?;

    let result = async {
        for command in setup_commands {
            let output = tokio::time::timeout(
                Duration::from_secs(operation_timeout_secs),
                exec_on_session(&mut session, command, None),
            )
            .await
            .map_err(|_| {
                format!("SSH command '{command}' timed out after {operation_timeout_secs}s")
            })??;

            if !output.success {
                return Err(format!(
                    "SSH command '{command}' failed: stdout='{}', stderr='{}'",
                    output.stdout.trim(),
                    output.stderr.trim()
                ));
            }
        }

        upload_file_on_session(
            &mut session,
            operation_timeout_secs,
            local_path,
            remote_path,
        )
        .await
    }
    .await;

    let _ = session
        .disconnect(Disconnect::ByApplication, "", "English")
        .await;
    result
}

async fn upload_file_on_session(
    session: &mut client::Handle<VerifyServerKey>,
    timeout_secs: u64,
    local_path: PathBuf,
    remote_path: String,
) -> Result<(), String> {
    let local_file = tokio::fs::File::open(&local_path)
        .await
        .map_err(|e| format!("Failed to open file {}: {e}", local_path.display()))?;
    let mut reader = tokio::io::BufReader::with_capacity(SFTP_BUF_SIZE, local_file);

    tokio::time::timeout(Duration::from_secs(timeout_secs), async {
        let channel = session
            .channel_open_session()
            .await
            .map_err(|e| format!("SFTP channel open failed: {e}"))?;
        channel
            .request_subsystem(true, "sftp")
            .await
            .map_err(|e| format!("SFTP subsystem request failed: {e}"))?;

        let sftp = SftpSession::new(channel.into_stream())
            .await
            .map_err(|e| format!("SFTP session creation failed: {e}"))?;
        sftp.set_timeout(timeout_secs).await;

        let mut attrs = FileAttributes::empty();
        attrs.permissions = Some(0o644);
        let mut remote_file = sftp
            .open_with_flags_and_attributes(
                remote_path,
                OpenFlags::CREATE | OpenFlags::TRUNCATE | OpenFlags::WRITE,
                attrs,
            )
            .await
            .map_err(|e| format!("SFTP open failed: {e}"))?;

        tokio::io::copy_buf(&mut reader, &mut remote_file)
            .await
            .map_err(|e| format!("SFTP write failed: {e}"))?;
        remote_file
            .flush()
            .await
            .map_err(|e| format!("SFTP flush failed: {e}"))?;
        remote_file
            .shutdown()
            .await
            .map_err(|e| format!("SFTP close failed: {e}"))?;

        let _ = sftp.close().await;
        Ok(())
    })
    .await
    .map_err(|_| format!("SFTP upload timed out after {timeout_secs}s"))
    .and_then(|result| result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use russh::keys::parse_public_key_base64;

    #[test]
    fn exec_output_success_tracks_zero_exit_status() {
        let output = SshExecOutput {
            success: true,
            stdout: "ok".to_string(),
            stderr: String::new(),
        };

        assert!(output.success);
        assert_eq!(output.stdout, "ok");
        assert!(output.stderr.is_empty());
    }

    #[test]
    fn default_known_hosts_path_uses_openssh_user_file() {
        assert_eq!(DEFAULT_KNOWN_HOSTS_RELATIVE_PATH, ".ssh/known_hosts");
    }

    fn test_key_one() -> PublicKey {
        parse_public_key_base64(
            "AAAAC3NzaC1lZDI1NTE5AAAAIJdD7y3aLq454yWBdwLWbieU1ebz9/cu7/QEXn9OIeZJ",
        )
        .expect("test key should parse")
    }

    fn test_key_two() -> PublicKey {
        parse_public_key_base64(
            "AAAAC3NzaC1lZDI1NTE5AAAAIA6rWI3G1sz07DnfFlrouTcysQlj2P+jpNSOEWD9OJ3X",
        )
        .expect("test key should parse")
    }

    #[test]
    fn tofu_learns_unknown_host_key_and_accepts_it_again() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("known_hosts");
        let policy =
            SshHostKeyPolicy::trust_on_first_use(Some(path.to_string_lossy().into_owned()));
        let key = test_key_one();

        policy
            .verify("example.test", 22, &key)
            .expect("tofu should learn first key");
        policy
            .verify("example.test", 22, &key)
            .expect("tofu should accept learned key");

        let known_hosts = std::fs::read_to_string(path).expect("known_hosts should be written");
        assert!(known_hosts.contains("example.test"));
    }

    #[test]
    fn tofu_creates_default_known_hosts_parent_directory() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("missing-parent").join("known_hosts");
        let policy =
            SshHostKeyPolicy::trust_on_first_use(Some(path.to_string_lossy().into_owned()));
        let key = test_key_one();

        policy
            .verify("example.test", 22, &key)
            .expect("tofu should create parent directory and learn first key");

        let known_hosts = std::fs::read_to_string(path).expect("known_hosts should be written");
        assert!(known_hosts.contains("example.test"));
    }

    #[test]
    fn tofu_rejects_changed_host_key() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("known_hosts");
        let policy =
            SshHostKeyPolicy::trust_on_first_use(Some(path.to_string_lossy().into_owned()));

        policy
            .verify("example.test", 22, &test_key_one())
            .expect("first key should be learned");
        let err = policy
            .verify("example.test", 22, &test_key_two())
            .expect_err("changed key should be rejected");

        assert!(err.contains("presented key does not match"));
    }

    #[test]
    fn strict_rejects_unknown_host_key() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("known_hosts");
        let policy = SshHostKeyPolicy::strict(Some(path.to_string_lossy().into_owned()));

        let err = policy
            .verify("example.test", 22, &test_key_one())
            .expect_err("strict mode should reject unknown key");

        assert!(err.contains("no matching key found"));
    }

    #[test]
    fn strict_accepts_known_host_key() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("known_hosts");
        let key = test_key_one();
        known_hosts::learn_known_hosts_path("example.test", 22, &key, &path)
            .expect("known_hosts seed should work");
        let policy = SshHostKeyPolicy::strict(Some(path.to_string_lossy().into_owned()));

        policy
            .verify("example.test", 22, &key)
            .expect("strict mode should accept known key");
    }

    #[test]
    fn insecure_policy_accepts_without_known_hosts() {
        let policy = SshHostKeyPolicy::insecure_accept_any();

        policy
            .verify("example.test", 22, &test_key_one())
            .expect("explicit insecure policy should accept any key");
    }
}
