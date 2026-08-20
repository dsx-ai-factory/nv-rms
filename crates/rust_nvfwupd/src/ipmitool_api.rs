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

//! IPMI command interface for firmware activation.
//!
//! Wraps `ipmitool` CLI invocations for power control and BMC reset
//! operations used during the firmware activation workflow.

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::LazyLock;
use std::time::Duration;

use thiserror::Error;
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::time::timeout;
use tracing::warn;

use crate::util::{BailAction, Util};

// ---------------------------------------------------------------------------
// IPMI command dictionary
// ---------------------------------------------------------------------------

/// Maps user-facing command names to their corresponding `ipmitool` sub-commands.
pub(crate) static IPMI_CMD_DICT: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut m = HashMap::new();
        m.insert("PWR_STATUS", "power status");
        m.insert("PWR_OFF", "power off");
        m.insert("PWR_ON", "power on");
        m.insert("PWR_CYCLE", "power cycle");
        m.insert("RESET_COLD", "mc reset cold");
        m.insert("RESET_WARM", "mc reset warm");
        m
    });

static IPMITOOL_AVAILABILITY: LazyLock<Mutex<HashMap<String, Result<(), String>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

const IPMITOOL_PROBE_TIMEOUT_SECS: u64 = 5;

/// Errors produced while invoking ipmitool for activation commands.
#[derive(Debug, Error, Clone, PartialEq)]
pub enum IpmiCommandError {
    /// ipmitool could not be found or failed the availability probe.
    #[error("ipmitool is not available: {0}")]
    IpmiNotAvailable(String),
    /// Failed to spawn or wait for the ipmitool subprocess.
    #[error("Error: {0}")]
    Spawn(String),
    /// Target connection values are invalid for ipmitool argv construction.
    #[error("{0}")]
    InvalidTarget(String),
    /// The ipmitool subprocess exceeded the command timeout.
    #[error("Error: command timed out after {0}s")]
    Timeout(f64),
}

// ---------------------------------------------------------------------------
// IpmiToolActivation
// ---------------------------------------------------------------------------

/// Manages IPMI-based BMC operations for firmware activation.
///
/// Holds BMC connection credentials and provides methods to execute
/// `ipmitool` commands via subprocess.
pub struct IpmiToolActivation {
    /// Configuration dictionary with keys `BMC_IP`, `BMC_USERNAME`, `BMC_PASSWORD`.
    pub conf_dict: HashMap<String, String>,
    /// Path or binary name used to invoke ipmitool.
    ipmitool_bin: String,
}

impl IpmiToolActivation {
    /// Create a new `IpmiToolActivation` with default (empty) credentials.
    pub fn new() -> Self {
        let mut conf = HashMap::new();
        conf.insert("BMC_IP".to_string(), String::new());
        conf.insert("BMC_USERNAME".to_string(), String::new());
        conf.insert("BMC_PASSWORD".to_string(), String::new());
        Self {
            conf_dict: conf,
            ipmitool_bin: std::env::var("NVFWUPD_IPMITOOL_BIN")
                .unwrap_or_else(|_| "ipmitool".to_string()),
        }
    }

    /// Spawn a subprocess to run an IPMI command via `ipmitool`.
    ///
    /// # Arguments
    /// * `command` - The ipmitool sub-command string (e.g. "power status")
    /// * `check` - If true, treat a non-zero exit code as failure
    /// * `timeout` - Optional timeout in seconds. When set, the child
    ///               process is killed if it exceeds the deadline.
    ///
    /// # Returns
    /// `(success, output_or_error)` where `output_or_error` contains either
    /// the stdout text on success or an error description on failure.
    pub async fn run_ipmi_command_subprocess(
        &self,
        command: &str,
        check: bool,
        timeout_secs: Option<f64>,
    ) -> Result<(bool, String), IpmiCommandError> {
        self.ensure_ipmitool_available().await?;

        let bmc_ip = self
            .conf_dict
            .get("BMC_IP")
            .map(|s| s.as_str())
            .unwrap_or("");
        let bmc_user = self
            .conf_dict
            .get("BMC_USERNAME")
            .map(|s| s.as_str())
            .unwrap_or("");
        let bmc_pass = self
            .conf_dict
            .get("BMC_PASSWORD")
            .map(|s| s.as_str())
            .unwrap_or("");

        Self::validate_ipmitool_target_value("ip", bmc_ip)
            .map_err(IpmiCommandError::InvalidTarget)?;
        Self::validate_ipmitool_target_value("user", bmc_user)
            .map_err(IpmiCommandError::InvalidTarget)?;
        Self::validate_ipmitool_target_value("password", bmc_pass)
            .map_err(IpmiCommandError::InvalidTarget)?;

        let mut cmd = Command::new(&self.ipmitool_bin);
        cmd.arg("-I")
            .arg("lanplus")
            .arg("-H")
            .arg(bmc_ip)
            .arg("-U")
            .arg(bmc_user)
            .arg("-E")
            .args(command.split_whitespace())
            .env("IPMI_PASSWORD", bmc_pass)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let output = if let Some(secs) = timeout_secs {
            if !secs.is_finite() || secs < 0.0 {
                return Err(IpmiCommandError::Timeout(secs));
            }
            let deadline = Duration::from_secs_f64(secs);
            cmd.kill_on_drop(true);
            let child = cmd
                .spawn()
                .map_err(|e| IpmiCommandError::Spawn(e.to_string()))?;
            timeout(deadline, child.wait_with_output())
                .await
                .map_err(|_| IpmiCommandError::Timeout(secs))?
                .map_err(|e| IpmiCommandError::Spawn(e.to_string()))?
        } else {
            cmd.output()
                .await
                .map_err(|e| IpmiCommandError::Spawn(e.to_string()))?
        };

        if check && !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            Ok((false, format!("Error: {}", stderr)))
        } else {
            let stdout = String::from_utf8_lossy(&output.stdout).to_string();
            Ok((output.status.success(), stdout))
        }
    }

    /// Parse BMC target arguments and populate `conf_dict`.
    ///
    /// Expects `target_args` to be key=value pairs such as
    /// `["ip=10.0.0.1", "user=admin", "password=secret"]`.
    pub fn setup_ipmi_command(&mut self, target_args: &[String]) {
        let mut arg_dict = HashMap::new();
        for each in target_args {
            let tokens: Vec<&str> = each.splitn(2, '=').collect();
            if tokens.len() < 2 {
                tracing::debug!("invalid IPMI target argument token count: {}", tokens.len());
                Util::bail_nvfwupd(
                    1,
                    &format!("Error: invalid target arguments: {}", each),
                    BailAction::DoNothing,
                    None,
                );
                return;
            }
            arg_dict.insert(tokens[0].to_string(), tokens[1].to_string());
        }

        for key in ["ip", "user", "password"] {
            if let Some(value) = arg_dict.get(key) {
                if let Err(message) = Self::validate_ipmitool_target_value(key, value) {
                    Util::bail_nvfwupd(1, &message, BailAction::DoNothing, None);
                    return;
                }
            }
        }

        self.conf_dict.insert(
            "BMC_IP".to_string(),
            arg_dict.get("ip").cloned().unwrap_or_default(),
        );
        self.conf_dict.insert(
            "BMC_USERNAME".to_string(),
            arg_dict.get("user").cloned().unwrap_or_default(),
        );
        self.conf_dict.insert(
            "BMC_PASSWORD".to_string(),
            arg_dict.get("password").cloned().unwrap_or_default(),
        );
    }

    fn validate_ipmitool_target_value(key: &str, value: &str) -> Result<(), String> {
        if value.starts_with('-') {
            Err(format!(
                "Error: invalid IPMI target argument: {} value must not start with '-'",
                key
            ))
        } else {
            Ok(())
        }
    }

    /// Run an IPMI command by its logical name and print the result.
    ///
    /// Looks up `command` in `IPMI_CMD_DICT` and executes the
    /// corresponding `ipmitool` invocation. Falls back to the `-C 17`
    /// cipher suite on initial failure.
    ///
    /// # Returns
    /// `0` on success, `1` on failure.
    pub async fn run_ipmi_command(&self, command: &str) -> Result<i32, IpmiCommandError> {
        let cmd_string = match IPMI_CMD_DICT.get(command) {
            Some(s) => *s,
            None => {
                let supported: Vec<&&str> = IPMI_CMD_DICT.keys().collect();
                Util::bail_nvfwupd(
                    1,
                    &format!(
                        "Error: invalid IPMI command input: {}. Supported commands {:?}",
                        command, supported
                    ),
                    BailAction::DoNothing,
                    None,
                );
                return Ok(1);
            }
        };

        match self.get_ipmi_output(cmd_string).await {
            Ok(code) => Ok(code),
            Err(IpmiCommandError::IpmiNotAvailable(e)) => {
                Err(IpmiCommandError::IpmiNotAvailable(e))
            }
            Err(e) => {
                println!("IPMI Command Status: Failed");
                println!("IPMI Error message: {}", e);
                Ok(1)
            }
        }
    }

    /// Execute an IPMI command string and print output or error.
    ///
    /// If the first attempt fails, retries with `-C 17` cipher suite.
    async fn get_ipmi_output(&self, cmd_str: &str) -> Result<i32, IpmiCommandError> {
        let (ok, output) = self
            .run_ipmi_command_subprocess(cmd_str, false, None)
            .await?;
        if !ok {
            // Retry with cipher suite 17
            let retry_cmd = format!("-C 17 {}", cmd_str);
            let (ok2, output2) = self
                .run_ipmi_command_subprocess(&retry_cmd, false, None)
                .await?;
            if ok2 {
                println!("IPMI Command Status: Success");
                println!("{}", output2);
                return Ok(0);
            }
            println!("IPMI Command Status: Failed");
            if !output.is_empty() {
                println!("IPMI Output: {}", output);
            }
            println!("IPMI Error message: {}", output2);
            return Ok(1);
        }

        println!("IPMI Command Status: Success");
        println!("{}", output);
        Ok(0)
    }

    async fn ensure_ipmitool_available(&self) -> Result<(), IpmiCommandError> {
        let cached = {
            let cache = IPMITOOL_AVAILABILITY.lock().await;
            cache.get(&self.ipmitool_bin).cloned()
        };
        if let Some(result) = cached {
            return result.map_err(IpmiCommandError::IpmiNotAvailable);
        }

        let result = self.probe_ipmitool_binary().await;
        if let Err(ref e) = result {
            warn!(ipmitool = %self.ipmitool_bin, error = %e, "ipmitool is not available");
        }

        let mut cache = IPMITOOL_AVAILABILITY.lock().await;
        cache.insert(self.ipmitool_bin.clone(), result.clone());
        result.map_err(IpmiCommandError::IpmiNotAvailable)
    }

    async fn probe_ipmitool_binary(&self) -> Result<(), String> {
        let mut cmd = Command::new(&self.ipmitool_bin);
        cmd.arg("-V").kill_on_drop(true);
        let output = timeout(
            Duration::from_secs(IPMITOOL_PROBE_TIMEOUT_SECS),
            cmd.output(),
        )
        .await
        .map_err(|_| format!("{} -V timed out", self.ipmitool_bin))?
        .map_err(|e| e.to_string())?;

        if output.status.success() {
            Ok(())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            if stderr.is_empty() {
                Err(format!(
                    "{} -V exited with status {}",
                    self.ipmitool_bin, output.status
                ))
            } else {
                Err(stderr)
            }
        }
    }
}

#[cfg(test)]
impl IpmiToolActivation {
    fn new_with_binary(ipmitool_bin: String) -> Self {
        let mut activation = Self::new();
        activation.ipmitool_bin = ipmitool_bin;
        activation
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use tempfile::TempDir;

    static IPMI_TEST_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    fn write_executable(path: &Path, script: &str) {
        {
            let mut file = fs::File::create(path).unwrap();
            file.write_all(script.as_bytes()).unwrap();
            file.flush().unwrap();
            file.sync_all().unwrap();
        }
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    fn fake_ipmitool(script: &str) -> (TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ipmitool");
        write_executable(&path, script);
        (dir, path.to_string_lossy().to_string())
    }

    fn configured_ipmi(ipmitool_bin: String) -> IpmiToolActivation {
        let mut ipmi = IpmiToolActivation::new_with_binary(ipmitool_bin);
        ipmi.conf_dict
            .insert("BMC_IP".to_string(), "10.0.0.1".to_string());
        ipmi.conf_dict
            .insert("BMC_USERNAME".to_string(), "admin".to_string());
        ipmi.conf_dict
            .insert("BMC_PASSWORD".to_string(), "secret".to_string());
        ipmi
    }

    #[test]
    fn test_ipmi_cmd_dict_entries() {
        assert_eq!(IPMI_CMD_DICT.get("PWR_STATUS"), Some(&"power status"));
        assert_eq!(IPMI_CMD_DICT.get("PWR_OFF"), Some(&"power off"));
        assert_eq!(IPMI_CMD_DICT.get("PWR_ON"), Some(&"power on"));
        assert_eq!(IPMI_CMD_DICT.get("PWR_CYCLE"), Some(&"power cycle"));
        assert_eq!(IPMI_CMD_DICT.get("RESET_COLD"), Some(&"mc reset cold"));
        assert_eq!(IPMI_CMD_DICT.get("RESET_WARM"), Some(&"mc reset warm"));
    }

    #[test]
    fn test_setup_ipmi_command() {
        let mut ipmi = IpmiToolActivation::new();
        let args = vec![
            "ip=10.0.0.1".to_string(),
            "user=admin".to_string(),
            "password=secret".to_string(),
        ];
        ipmi.setup_ipmi_command(&args);
        assert_eq!(ipmi.conf_dict.get("BMC_IP").unwrap(), "10.0.0.1");
        assert_eq!(ipmi.conf_dict.get("BMC_USERNAME").unwrap(), "admin");
        assert_eq!(ipmi.conf_dict.get("BMC_PASSWORD").unwrap(), "secret");
    }

    #[test]
    fn ipmitool_target_value_validation_rejects_option_shaped_values() {
        for key in ["ip", "user", "password"] {
            let error =
                IpmiToolActivation::validate_ipmitool_target_value(key, "-flag").unwrap_err();
            assert!(error.contains(key));
            assert!(error.contains("must not start with '-'"));
        }

        assert!(IpmiToolActivation::validate_ipmitool_target_value("user", "admin").is_ok());
        assert!(IpmiToolActivation::validate_ipmitool_target_value("password", "").is_ok());
    }

    #[test]
    fn setup_ipmi_command_rejects_option_shaped_values_without_storing() {
        let mut ipmi = IpmiToolActivation::new();
        ipmi.conf_dict
            .insert("BMC_IP".to_string(), "unchanged".to_string());
        let args = vec![
            "ip=10.0.0.1".to_string(),
            "user=-X".to_string(),
            "password=secret".to_string(),
        ];

        ipmi.setup_ipmi_command(&args);

        assert_eq!(ipmi.conf_dict.get("BMC_IP").unwrap(), "unchanged");
        assert_eq!(ipmi.conf_dict.get("BMC_USERNAME").unwrap(), "");
        assert_eq!(ipmi.conf_dict.get("BMC_PASSWORD").unwrap(), "");
    }

    #[tokio::test]
    async fn test_run_ipmi_command_invalid() {
        let ipmi = IpmiToolActivation::new();
        // Invalid command name should return 1
        let result = ipmi.run_ipmi_command("INVALID_CMD").await.unwrap();
        assert_eq!(result, 1);
    }

    #[tokio::test]
    async fn run_ipmi_command_subprocess_reports_missing_binary() {
        let _guard = IPMI_TEST_LOCK.lock().await;
        let ipmi = configured_ipmi("/tmp/nvfwupd-ipmitool-definitely-missing".to_string());

        let result = ipmi
            .run_ipmi_command_subprocess("power status", false, Some(1.0))
            .await;

        assert!(matches!(result, Err(IpmiCommandError::IpmiNotAvailable(_))));
    }

    #[tokio::test]
    async fn run_ipmi_command_subprocess_uses_async_command_and_captures_stdout() {
        let _guard = IPMI_TEST_LOCK.lock().await;
        let (_dir, bin) = fake_ipmitool(
            r#"#!/bin/sh
if [ "$1" = "-V" ]; then
  echo "ipmitool version"
  exit 0
fi
printf '%s\n' "$@"
exit 0
"#,
        );
        let ipmi = configured_ipmi(bin);

        let (ok, output) = ipmi
            .run_ipmi_command_subprocess("power status", true, Some(2.0))
            .await
            .unwrap();

        assert!(ok);
        assert!(output.contains("-I"));
        assert!(output.contains("lanplus"));
        assert!(output.contains("power"));
        assert!(output.contains("status"));
    }

    #[tokio::test]
    async fn run_ipmi_command_subprocess_uses_env_password_not_argv() {
        let _guard = IPMI_TEST_LOCK.lock().await;
        let (_dir, bin) = fake_ipmitool(
            r#"#!/bin/sh
if [ "$1" = "-V" ]; then
  echo "ipmitool version"
  exit 0
fi
printf 'args=%s\n' "$*"
printf 'env_password=%s\n' "$IPMI_PASSWORD"
exit 0
"#,
        );
        let mut ipmi = configured_ipmi(bin);
        ipmi.conf_dict
            .insert("BMC_PASSWORD".to_string(), "plain_secret".to_string());

        let (ok, output) = ipmi
            .run_ipmi_command_subprocess("power status", true, Some(2.0))
            .await
            .unwrap();

        assert!(ok);
        assert!(output.contains("-E"));
        assert!(!output.contains("-P"));
        assert!(output.contains("env_password=plain_secret"));
        let args_line = output
            .lines()
            .find(|line| line.starts_with("args="))
            .unwrap();
        assert!(!args_line.contains("plain_secret"));
    }

    #[tokio::test]
    async fn run_ipmi_command_subprocess_rejects_option_shaped_conf_dict_values() {
        let _guard = IPMI_TEST_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("calls.log");
        let script = format!(
            r#"#!/bin/sh
if [ "$1" = "-V" ]; then
  exit 0
fi
printf '%s\n' "$*" >> '{}'
exit 0
"#,
            log_path.display()
        );
        let path = dir.path().join("ipmitool");
        write_executable(&path, &script);

        let mut ipmi = configured_ipmi(path.to_string_lossy().to_string());
        ipmi.conf_dict
            .insert("BMC_USERNAME".to_string(), "-X".to_string());

        let result = ipmi
            .run_ipmi_command_subprocess("power status", true, Some(2.0))
            .await;

        match result {
            Err(IpmiCommandError::InvalidTarget(message)) => {
                assert!(message.contains("user value must not start with '-'"));
            }
            other => panic!("expected invalid target error, got {other:?}"),
        }
        assert!(!log_path.exists());
    }

    #[tokio::test]
    async fn run_ipmi_command_subprocess_times_out() {
        let _guard = IPMI_TEST_LOCK.lock().await;
        let (_dir, bin) = fake_ipmitool(
            r#"#!/bin/sh
if [ "$1" = "-V" ]; then
  exit 0
fi
sleep 5
"#,
        );
        let ipmi = configured_ipmi(bin);

        let result = ipmi
            .run_ipmi_command_subprocess("power status", false, Some(0.0))
            .await;

        assert!(matches!(result, Err(IpmiCommandError::Timeout(_))));
    }

    #[tokio::test]
    async fn run_ipmi_command_retries_with_cipher_suite_17() {
        let _guard = IPMI_TEST_LOCK.lock().await;
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("calls.log");
        let script = format!(
            r#"#!/bin/sh
if [ "$1" = "-V" ]; then
  exit 0
fi
printf '%s\n' "$*" >> '{}'
case "$*" in
  *"-C 17"*) echo "retry ok"; exit 0 ;;
  *) echo "first attempt failed"; exit 1 ;;
esac
"#,
            log_path.display()
        );
        let path = dir.path().join("ipmitool");
        write_executable(&path, &script);

        let ipmi = configured_ipmi(path.to_string_lossy().to_string());

        let result = ipmi.run_ipmi_command("PWR_STATUS").await.unwrap();

        assert_eq!(result, 0);
        let calls = fs::read_to_string(log_path).unwrap();
        let lines: Vec<&str> = calls.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(!lines[0].contains("-C 17"));
        assert!(lines[1].contains("-C 17"));
    }
}
