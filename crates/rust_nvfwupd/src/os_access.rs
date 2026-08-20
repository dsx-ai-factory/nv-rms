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

//! Host OS SSH access module for nvfwupd.
//!
//! Provides [`OsAccess`] for executing commands and transferring files
//! to a remote host OS over SSH/SFTP. Used primarily by the flint update
//! command for ConnectX/BlueField firmware operations.

use std::collections::HashMap;
use std::fmt;
use std::net::IpAddr;
use std::path::Path;
use std::str::FromStr;

use serde_json::Value;
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

use crate::ssh_options::{
    self, SSH_HOST_KEY_MODE_ARG, SSH_HOST_KEY_MODE_DISABLED, SSH_HOST_KEY_MODE_STRICT,
    SSH_HOST_KEY_MODE_TOFU, SSH_KNOWN_HOSTS_ARG,
};
use crate::ssh_transport;

// ---------------------------------------------------------------------------
// OsAccess
// ---------------------------------------------------------------------------

/// Implements Host OS endpoint access via SSH.
///
/// Provides command execution and file transfer capabilities to a remote
/// server identified by IP address and login credentials.
#[derive(Clone)]
pub struct OsAccess {
    /// IP address of the target host.
    pub ip: String,
    /// SSH username.
    pub user: String,
    /// SSH password.
    pub password: String,
    /// SSH port (default: 22).
    pub port: u16,
    /// Optional server type identifier.
    pub server_type: Option<String>,
    /// Optional known_hosts file for SSH/SFTP host-key verification.
    pub ssh_known_hosts: Option<String>,
    /// SSH/SFTP host-key verification mode: `disabled`, `tofu`, or `strict`.
    pub ssh_host_key_mode: String,
    /// Tracks whether the port value was invalid in the input dict.
    /// Causes `is_valid()` to report a port error (matching Python).
    invalid_port_raw: Option<String>,
    /// Tracks invalid SSH host verification input until `is_valid()`.
    invalid_ssh_option: Option<String>,
}

impl fmt::Debug for OsAccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OsAccess")
            .field("ip", &self.ip)
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .field("port", &self.port)
            .field("server_type", &self.server_type)
            .field("ssh_known_hosts", &self.ssh_known_hosts)
            .field("ssh_host_key_mode", &self.ssh_host_key_mode)
            .field("invalid_port_raw", &self.invalid_port_raw)
            .field("invalid_ssh_option", &self.invalid_ssh_option)
            .finish()
    }
}

impl OsAccess {
    /// Create a new `OsAccess` from a key-value argument dictionary.
    ///
    /// Expected keys: `ip`, `user`, `password`, `port` (optional, default 22),
    /// `servertype` (optional).
    pub fn from_arg_dict(arg_dict: &HashMap<String, String>) -> Self {
        // Python validates port and returns error if invalid. We defer the
        // error to `is_valid()` by tracking the invalid raw input.
        let (port, invalid_port_raw) = match arg_dict.get("port") {
            Some(p) => match p.parse::<u16>() {
                Ok(v) if v >= 1 => (v, None),
                _ => (22, Some(p.clone())),
            },
            None => (22, None),
        };

        let (ssh_known_hosts, ssh_host_key_mode, invalid_ssh_option) =
            match ssh_options::parse_ssh_options(arg_dict) {
                Ok(options) => (options.known_hosts, options.mode, None),
                Err(message) => (None, SSH_HOST_KEY_MODE_DISABLED.to_string(), Some(message)),
            };

        Self {
            ip: arg_dict.get("ip").cloned().unwrap_or_default(),
            user: arg_dict.get("user").cloned().unwrap_or_default(),
            password: arg_dict.get("password").cloned().unwrap_or_default(),
            port,
            server_type: arg_dict.get("servertype").cloned(),
            ssh_known_hosts,
            ssh_host_key_mode,
            invalid_port_raw,
            invalid_ssh_option,
        }
    }

    /// Validate that all required fields are present and correctly formatted.
    ///
    /// Returns `(is_valid, message)`.
    pub fn is_valid(&self) -> (bool, String) {
        if self.ip.is_empty() {
            return (false, "OS IP address is required".to_string());
        }
        if self.user.is_empty() {
            return (false, "OS username is required".to_string());
        }
        if self.password.is_empty() {
            return (false, "OS password is required".to_string());
        }

        // Validate IP address format
        if IpAddr::from_str(&self.ip).is_err() {
            return (false, format!("Invalid OS IP address format: {}", self.ip));
        }

        // Report invalid port input from from_arg_dict (matches Python bail behavior)
        if let Some(ref raw) = self.invalid_port_raw {
            return (false, format!("Invalid port number format: {}", raw));
        }
        if let Some(ref message) = self.invalid_ssh_option {
            return (false, message.clone());
        }

        // Validate port range (Python: 1..65535; u16 caps at 65535)
        if self.port < 1 {
            return (false, format!("Invalid port number: {}", self.port));
        }
        // Note: u16 type already prevents > 65535

        (true, "Valid OS access parameters".to_string())
    }

    /// Check whether the remote host's SSH TCP port is reachable.
    pub async fn is_reachable(&self) -> (bool, String) {
        let host = match self.normalized_host() {
            Ok(host) => host,
            Err(e) => return (false, format!("OS connection error: {e}")),
        };

        match timeout(
            Duration::from_secs(30),
            TcpStream::connect((host.as_str(), self.port)),
        )
        .await
        {
            Ok(Ok(_stream)) => (true, "OS is reachable".to_string()),
            Ok(Err(e)) => (false, format!("OS is not reachable: {e}")),
            Err(_) => (false, "OS connection timed out after 30s".to_string()),
        }
    }

    /// Execute a command on the remote host via SSH.
    ///
    /// # Arguments
    /// * `command` - Shell command to execute
    /// * `timeout` - Timeout in seconds for the TCP connection
    /// * `use_sudo` - If true, prefix with `sudo -S` and supply password via stdin
    ///
    /// # Returns
    /// `(success, stdout, stderr)`
    pub async fn execute_command(
        &self,
        command: &str,
        timeout: u64,
        use_sudo: bool,
    ) -> (bool, String, String) {
        let actual_cmd = if use_sudo {
            format!("sudo -S {} 2>/dev/null", command)
        } else {
            command.to_string()
        };

        let host = match self.normalized_host() {
            Ok(host) => host,
            Err(e) => {
                return (
                    false,
                    String::new(),
                    format!("Command execution error: {}", e),
                );
            }
        };
        let stdin = use_sudo.then(|| format!("{}\n", self.password));

        match ssh_transport::execute_command_async(
            &host,
            self.port,
            &self.user,
            &self.password,
            timeout,
            &actual_cmd,
            stdin,
            self.ssh_host_key_policy(),
        )
        .await
        {
            Ok(output) => (output.success, output.stdout, output.stderr),
            Err(e) => (
                false,
                String::new(),
                format!("Command execution error: {}", e),
            ),
        }
    }

    /// Transfer a local file to the remote host via SFTP.
    ///
    /// # Arguments
    /// * `local_path` - Path to the local file
    /// * `remote_file` - Optional remote path; defaults to basename of local_path
    /// * `timeout` - Connection timeout in seconds (default: 300)
    ///
    /// # Returns
    /// `(success, message, error_message)`
    pub async fn transfer_file(
        &self,
        local_path: &str,
        remote_file: Option<&str>,
        timeout: u64,
    ) -> (bool, String, String) {
        let local = Path::new(local_path);
        if let Err(e) = tokio::fs::metadata(local).await {
            return (
                false,
                String::new(),
                format!("Local file not found: {} ({})", local_path, e),
            );
        }

        let remote_name = remote_file.map(|s| s.to_string()).unwrap_or_else(|| {
            local
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| local_path.to_string())
        });

        let host = match self.normalized_host() {
            Ok(host) => host,
            Err(e) => {
                return (false, String::new(), format!("File transfer error: {}", e));
            }
        };

        match ssh_transport::upload_file_async(
            &host,
            self.port,
            &self.user,
            &self.password,
            timeout,
            local.to_path_buf(),
            remote_name.clone(),
            self.ssh_host_key_policy(),
        )
        .await
        {
            Ok(()) => (
                true,
                format!(
                    "File {} transferred successfully to {}",
                    local_path, remote_name
                ),
                String::new(),
            ),
            Err(e) => (false, String::new(), format!("SFTP upload error: {}", e)),
        }
    }

    /// Return connection information as a human-readable string.
    pub fn get_connection_info(&self) -> String {
        format!(
            "ip={}, user={}, port={}, server_type={}",
            self.ip,
            self.user,
            self.port,
            self.server_type.as_deref().unwrap_or("N/A")
        )
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn normalized_host(&self) -> Result<String, String> {
        // Validate the address while returning the unbracketed host string
        // expected by russh's async resolver.
        let ip_stripped = self.ip.replace('[', "").replace(']', "");
        IpAddr::from_str(&ip_stripped)
            .map_err(|e| format!("Invalid IP address {}: {}", self.ip, e))?;
        Ok(ip_stripped)
    }

    fn ssh_host_key_policy(&self) -> ssh_transport::SshHostKeyPolicy {
        ssh_options::host_key_policy(self.ssh_known_hosts.clone(), &self.ssh_host_key_mode)
    }
}

// ---------------------------------------------------------------------------
// Factory function
// ---------------------------------------------------------------------------

/// Parse global CLI arguments and produce an [`OsAccess`] instance.
///
/// The `global_args` are expected to contain an `os_target` field with
/// key=value pairs (e.g. `ip=10.0.0.1 user=admin password=secret`).
///
/// # Returns
/// `Ok((OsAccess, arg_dict))` on success, or `Err(message)` on failure.
pub fn get_os_access(
    os_target_args: &[String],
    _json_dict: Option<&Value>,
) -> Result<(OsAccess, HashMap<String, String>), String> {
    let mut arg_dict = HashMap::new();

    for each in os_target_args {
        let tokens: Vec<&str> = each.splitn(2, '=').collect();
        if tokens.len() < 2 {
            return Err(format!(
                "Error: invalid OS target arguments: {}, token length: {}",
                each,
                tokens.len()
            ));
        }
        arg_dict.insert(tokens[0].to_ascii_lowercase(), tokens[1].to_string());
    }

    let os_access = OsAccess::from_arg_dict(&arg_dict);
    let (valid, msg) = os_access.is_valid();

    if !valid {
        return Err(format!("Error: {}", msg));
    }

    Ok((os_access, arg_dict))
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;

    fn assert_send_future<F: Future + Send>(future: F) {
        drop(future);
    }

    fn valid_os_access() -> OsAccess {
        OsAccess {
            ip: "127.0.0.1".to_string(),
            user: "admin".to_string(),
            password: "pass".to_string(),
            port: 22,
            server_type: None,
            ssh_known_hosts: None,
            ssh_host_key_mode: SSH_HOST_KEY_MODE_TOFU.to_string(),
            invalid_port_raw: None,
            invalid_ssh_option: None,
        }
    }

    #[test]
    fn test_debug_redacts_password() {
        let mut os_access = valid_os_access();
        os_access.password = "plain_secret".to_string();

        let debug = format!("{os_access:?}");
        assert!(!debug.contains("plain_secret"));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn test_is_valid_missing_ip() {
        let oa = OsAccess {
            ip: String::new(),
            user: "admin".to_string(),
            password: "pass".to_string(),
            port: 22,
            server_type: None,
            ssh_known_hosts: None,
            ssh_host_key_mode: SSH_HOST_KEY_MODE_TOFU.to_string(),
            invalid_port_raw: None,
            invalid_ssh_option: None,
        };
        let (valid, _) = oa.is_valid();
        assert!(!valid);
    }

    #[test]
    fn test_is_valid_bad_ip() {
        let oa = OsAccess {
            ip: "not_an_ip".to_string(),
            user: "admin".to_string(),
            password: "pass".to_string(),
            port: 22,
            server_type: None,
            ssh_known_hosts: None,
            ssh_host_key_mode: SSH_HOST_KEY_MODE_TOFU.to_string(),
            invalid_port_raw: None,
            invalid_ssh_option: None,
        };
        let (valid, msg) = oa.is_valid();
        assert!(!valid);
        assert!(msg.contains("Invalid OS IP address"));
    }

    #[test]
    fn test_is_valid_ok() {
        let oa = OsAccess {
            ip: "192.168.1.1".to_string(),
            user: "admin".to_string(),
            password: "pass".to_string(),
            port: 22,
            server_type: None,
            ssh_known_hosts: None,
            ssh_host_key_mode: SSH_HOST_KEY_MODE_TOFU.to_string(),
            invalid_port_raw: None,
            invalid_ssh_option: None,
        };
        let (valid, _) = oa.is_valid();
        assert!(valid);
    }

    #[test]
    fn test_get_connection_info() {
        let oa = OsAccess {
            ip: "10.0.0.1".to_string(),
            user: "root".to_string(),
            password: "secret".to_string(),
            port: 2222,
            server_type: Some("dgx".to_string()),
            ssh_known_hosts: None,
            ssh_host_key_mode: SSH_HOST_KEY_MODE_TOFU.to_string(),
            invalid_port_raw: None,
            invalid_ssh_option: None,
        };
        let info = oa.get_connection_info();
        assert!(info.contains("10.0.0.1"));
        assert!(info.contains("2222"));
        assert!(info.contains("dgx"));
    }

    #[test]
    fn test_get_os_access_parse() {
        let args = vec![
            "ip=192.168.1.1".to_string(),
            "user=admin".to_string(),
            "password=secret".to_string(),
        ];
        let result = get_os_access(&args, None);
        assert!(result.is_ok());
        let (oa, dict) = result.unwrap();
        assert_eq!(oa.ip, "192.168.1.1");
        assert_eq!(dict.get("user").unwrap(), "admin");
    }

    #[test]
    fn test_get_os_access_defaults_to_disabled_ssh_host_verification() {
        let args = vec![
            "ip=192.168.1.1".to_string(),
            "user=admin".to_string(),
            "password=secret".to_string(),
        ];
        let (oa, _) = get_os_access(&args, None).unwrap();

        assert_eq!(oa.ssh_known_hosts, None);
        assert_eq!(oa.ssh_host_key_mode, SSH_HOST_KEY_MODE_DISABLED);
    }

    #[test]
    fn test_get_os_access_accepts_ssh_host_key_options() {
        let args = vec![
            "ip=192.168.1.1".to_string(),
            "user=admin".to_string(),
            "password=secret".to_string(),
            "ssh_known_hosts=/tmp/known_hosts".to_string(),
            "ssh_host_key_mode=strict".to_string(),
        ];
        let (oa, _) = get_os_access(&args, None).unwrap();

        assert_eq!(oa.ssh_known_hosts.as_deref(), Some("/tmp/known_hosts"));
        assert_eq!(oa.ssh_host_key_mode, SSH_HOST_KEY_MODE_STRICT);
    }

    #[test]
    fn test_get_os_access_rejects_conflicting_ssh_host_key_options() {
        let args = vec![
            "ip=192.168.1.1".to_string(),
            "user=admin".to_string(),
            "password=secret".to_string(),
            "ssh_host_key_mode=disabled".to_string(),
            "ssh_known_hosts=/tmp/known_hosts".to_string(),
        ];

        let err = get_os_access(&args, None).unwrap_err();

        assert!(err.contains(SSH_KNOWN_HOSTS_ARG));
        assert!(err.contains(SSH_HOST_KEY_MODE_DISABLED));
    }

    #[test]
    fn phase_2e_os_access_io_methods_are_async_send() {
        let oa = valid_os_access();

        assert_send_future(oa.is_reachable());
        assert_send_future(oa.execute_command("true", 1, false));
        assert_send_future(oa.transfer_file("/tmp/nonexistent-fw-image.bin", None, 1));
    }

    #[tokio::test]
    async fn is_reachable_reports_invalid_ip_without_ssh_handshake() {
        let mut oa = valid_os_access();
        oa.ip = "not_an_ip".to_string();

        let (reachable, msg) = oa.is_reachable().await;

        assert!(!reachable);
        assert!(msg.contains("Invalid IP address"));
    }

    #[tokio::test]
    async fn transfer_file_missing_local_file_fails_before_sftp() {
        let oa = valid_os_access();

        let (success, output, error) = oa
            .transfer_file("/tmp/nvfwupd-definitely-missing-fw.bin", None, 1)
            .await;

        assert!(!success);
        assert!(output.is_empty());
        assert!(error.contains("Local file not found"));
    }
}
