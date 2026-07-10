/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: LicenseRef-NvidiaProprietary
 *
 * NVIDIA CORPORATION, its affiliates and licensors retain all intellectual
 * property and proprietary rights in and to this material, related
 * documentation and any modifications thereto. Any use, reproduction,
 * disclosure or distribution of this material and related documentation
 * without an express license agreement from NVIDIA CORPORATION or
 * its affiliates is strictly prohibited.
 */

//! Password-based SSH/SFTP transport helpers.
//!
//! Provides async SSH command execution and SFTP upload helpers built on
//! `russh` / `russh-sftp`.

use std::path::PathBuf;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::time::Duration;

#[cfg(test)]
use once_cell::sync::Lazy;
use russh::{client, ChannelMsg, Disconnect};
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::{FileAttributes, OpenFlags};
use tokio::io::AsyncWriteExt;

const SFTP_BUF_SIZE: usize = 128 * 1024;

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
static MOCK_SSH: Lazy<Mutex<std::collections::HashMap<String, MockSshState>>> =
    Lazy::new(|| Mutex::new(std::collections::HashMap::new()));

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

struct AcceptAnyServerKey;

impl client::Handler for AcceptAnyServerKey {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &russh::keys::PublicKey,
    ) -> Result<bool, Self::Error> {
        Ok(true)
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
) -> Result<SshExecOutput, String> {
    #[cfg(test)]
    if let Some(result) = mock_exec_result(host, command, stdin.clone()) {
        return result;
    }

    let mut session = connect_password(host, port, user, password, timeout_secs).await?;
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
) -> Result<client::Handle<AcceptAnyServerKey>, String> {
    let config = client::Config {
        inactivity_timeout: Some(Duration::from_secs(timeout_secs)),
        ..Default::default()
    };
    let connect = client::connect(Arc::new(config), (host, port), AcceptAnyServerKey);
    let mut session = tokio::time::timeout(Duration::from_secs(timeout_secs), connect)
        .await
        .map_err(|_| format!("SSH connect timed out after {timeout_secs}s"))?
        .map_err(|e| format!("SSH connect failed: {e}"))?;

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
    session: &mut client::Handle<AcceptAnyServerKey>,
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
) -> Result<(), String> {
    #[cfg(test)]
    if let Some(result) = mock_upload_result(host, &local_path, &remote_path) {
        return result;
    }

    let local_file = tokio::fs::File::open(&local_path)
        .await
        .map_err(|e| format!("Failed to open file {}: {e}", local_path.display()))?;
    let mut reader = tokio::io::BufReader::with_capacity(SFTP_BUF_SIZE, local_file);

    let mut session = connect_password(host, port, user, password, timeout_secs).await?;
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
) -> Result<(), String> {
    #[cfg(test)]
    if let Some(result) =
        mock_upload_with_setup_result(host, setup_commands, &local_path, &remote_path)
    {
        return result;
    }

    let mut session = connect_password(host, port, user, password, connect_timeout_secs).await?;

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
    session: &mut client::Handle<AcceptAnyServerKey>,
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
}
