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

//! YAML configuration file parser for nvfwupd.
//!
//! Reads a user-supplied YAML config file that defines one or more BMC
//! targets and their credentials, then exposes a list of target
//! dictionaries consumed by the update pipeline. Config parsing accepts
//! unquoted boolean-like values such as `on`, `off`, `yes`, and `no` for
//! user-facing boolean settings.

use std::fmt;
use std::path::Path;

use noyalib::compat::serde_yaml;
use noyalib::{ParserConfig, YamlVersion};
use serde_json::Value;

use crate::utils::Util as NvUtils;

// ---------------------------------------------------------------------------
// ConfigParser
// ---------------------------------------------------------------------------

/// Parses a YAML configuration file and produces a list of target entries.
///
/// The config file may contain either:
/// - A top-level `Targets` list (multi-target mode), or
/// - Flat keys `BMC_IP`, `RF_USERNAME`, `RF_PASSWORD`, and optionally
///   `TUNNEL_TCP_PORT`, `VERIFY_TLS`, `BMC_CA_CERT`, `SSH_KNOWN_HOSTS`, and
///   `SSH_HOST_KEY_MODE` (single-target shorthand).
#[derive(Clone)]
pub struct ConfigParser {
    /// The full parsed YAML content (as a JSON [`Value`] for uniform access).
    pub config_dict: Option<Value>,
    /// Resolved list of target dictionaries.
    pub targets: Vec<Value>,
    /// Filesystem path to the YAML config file.
    pub config_file_path: String,
}

impl fmt::Debug for ConfigParser {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let config_dict = self
            .config_dict
            .as_ref()
            .map(NvUtils::redact_secret_json_value);
        let targets: Vec<Value> = self
            .targets
            .iter()
            .map(NvUtils::redact_secret_json_value)
            .collect();

        f.debug_struct("ConfigParser")
            .field("config_dict", &config_dict)
            .field("targets", &targets)
            .field("config_file_path", &self.config_file_path)
            .finish()
    }
}

impl ConfigParser {
    /// Create a new `ConfigParser` for the given file path.
    pub fn new(config_file_path: String) -> Self {
        Self {
            config_dict: None,
            targets: Vec::new(),
            config_file_path,
        }
    }

    /// Read and parse the YAML configuration file.
    ///
    /// After successful parsing the target list is populated via
    /// `make_targets_list`.
    ///
    /// # Errors
    ///
    /// Returns an error string if the file does not exist, cannot be read,
    /// contains invalid YAML, or is empty.
    pub async fn parse_config_data(&mut self) -> Result<(), String> {
        let path = Path::new(&self.config_file_path);

        let metadata = tokio::fs::metadata(path)
            .await
            .map_err(|_| format!("Config file {} does not exist.", self.config_file_path))?;

        if !metadata.is_file() {
            return Err(format!(
                "Config file {} does not exist.",
                self.config_file_path
            ));
        }

        let contents = tokio::fs::read_to_string(path).await.map_err(|e| {
            format!(
                "Unable to read config file {}: {}",
                self.config_file_path, e
            )
        })?;

        // User config files may use YAML 1.1 boolean-like scalars for boolean
        // settings, such as `ParallelUpdate: on`, `VERIFY_TLS: off`, and
        // `SANITIZE_LOG: no`.
        let yaml_config = ParserConfig::new().version(YamlVersion::V1_1);
        let yaml_value: serde_yaml::Value = noyalib::from_str_with_config(&contents, &yaml_config)
            .map_err(|e| {
                format!(
                    "Unable to parse config file {}. \
                     Please provide valid YAML config: {}",
                    self.config_file_path, e
                )
            })?;

        // Convert YAML Value to serde_json::Value for uniform handling.
        let json_value: Value = serde_json::to_value(&yaml_value).map_err(|e| {
            format!(
                "Unable to convert YAML to JSON for config file {}: {}",
                self.config_file_path, e
            )
        })?;

        if json_value.is_null() {
            return Err(format!(
                "Config file {} is empty. Please provide valid YAML config",
                self.config_file_path
            ));
        }

        self.config_dict = Some(json_value);
        self.make_targets_list()?;
        Ok(())
    }

    /// Build the targets list from the parsed configuration.
    ///
    /// If a `Targets` key exists its value is used directly; otherwise a
    /// single target is synthesised from the flat `BMC_IP`, `RF_USERNAME`,
    /// `RF_PASSWORD` (and optional `TUNNEL_TCP_PORT`, `VERIFY_TLS`, and
    /// `BMC_CA_CERT`, `SSH_KNOWN_HOSTS`, and `SSH_HOST_KEY_MODE`) keys.
    fn make_targets_list(&mut self) -> Result<(), String> {
        let config = match &self.config_dict {
            Some(c) => c,
            None => return Ok(()),
        };

        if let Some(targets) = config.get("Targets") {
            if let Some(arr) = targets.as_array() {
                self.targets = arr.clone();
                return Ok(());
            }
            return Err(format!(
                "Config file {} has invalid Targets configuration. Targets must be a list.",
                self.config_file_path
            ));
        }

        // Single-target fallback: collect known keys into one map.
        let mut target = serde_json::Map::new();

        if let Some(v) = config.get("BMC_IP") {
            target.insert("BMC_IP".to_string(), v.clone());
        }
        if let Some(v) = config.get("RF_USERNAME") {
            target.insert("RF_USERNAME".to_string(), v.clone());
        }
        if let Some(v) = config.get("RF_PASSWORD") {
            target.insert("RF_PASSWORD".to_string(), v.clone());
        }
        if let Some(v) = config.get("TUNNEL_TCP_PORT") {
            target.insert("TUNNEL_TCP_PORT".to_string(), v.clone());
        }
        if let Some(v) = config.get("VERIFY_TLS") {
            target.insert("VERIFY_TLS".to_string(), v.clone());
        }
        if let Some(v) = config.get("BMC_CA_CERT") {
            target.insert("BMC_CA_CERT".to_string(), v.clone());
        }
        if let Some(v) = config.get("SSH_KNOWN_HOSTS") {
            target.insert("SSH_KNOWN_HOSTS".to_string(), v.clone());
        }
        if let Some(v) = config.get("SSH_HOST_KEY_MODE") {
            target.insert("SSH_HOST_KEY_MODE".to_string(), v.clone());
        }

        if target.is_empty() {
            if config
                .as_object()
                .is_some_and(|obj| obj.len() == 1 && obj.contains_key("SANITIZE_LOG"))
            {
                self.targets.clear();
                return Ok(());
            }
            return Err(format!(
                "Config file {} does not contain Targets or flat target keys.",
                self.config_file_path
            ));
        }

        let missing_keys: Vec<&str> = ["BMC_IP", "RF_USERNAME", "RF_PASSWORD"]
            .into_iter()
            .filter(|key| {
                target
                    .get(*key)
                    .and_then(|value| value.as_str())
                    .is_none_or(str::is_empty)
            })
            .collect();
        if !missing_keys.is_empty() {
            return Err(format!(
                "Config file {} has incomplete flat target configuration. Missing or invalid keys: {}.",
                self.config_file_path,
                missing_keys.join(", ")
            ));
        }

        self.targets = vec![Value::Object(target)];
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ConfigParser;

    #[tokio::test]
    async fn parse_config_data_reads_targets_from_yaml() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let config_path = dir.path().join("config.yaml");
        std::fs::write(
            &config_path,
            "\
Targets:
  - BMC_IP: 192.0.2.10
    RF_USERNAME: admin
    RF_PASSWORD: password
",
        )
        .expect("failed to write config");

        let mut parser = ConfigParser::new(config_path.to_string_lossy().into_owned());
        parser
            .parse_config_data()
            .await
            .expect("config should parse");

        assert!(parser.config_dict.is_some());
        assert_eq!(parser.targets.len(), 1);
        assert_eq!(parser.targets[0]["BMC_IP"], "192.0.2.10");
    }

    #[tokio::test]
    async fn parse_config_data_rejects_malformed_targets_key() {
        for (name, contents) in [
            (
                "scalar",
                "\
Targets: invalid
BMC_IP: 192.0.2.10
RF_USERNAME: admin
RF_PASSWORD: password
",
            ),
            (
                "null",
                "\
Targets:
BMC_IP: 192.0.2.10
RF_USERNAME: admin
RF_PASSWORD: password
",
            ),
        ] {
            let dir = tempfile::tempdir().expect("failed to create temp dir");
            let config_path = dir.path().join(format!("{name}.yaml"));
            std::fs::write(&config_path, contents).expect("failed to write config");

            let mut parser = ConfigParser::new(config_path.to_string_lossy().into_owned());
            let err = parser
                .parse_config_data()
                .await
                .expect_err("malformed Targets should not fall back to flat keys");

            assert!(err.contains("invalid Targets configuration"));
            assert!(err.contains("must be a list"));
            assert!(parser.targets.is_empty());
        }
    }

    #[tokio::test]
    async fn debug_redacts_config_passwords() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let config_path = dir.path().join("config.yaml");
        std::fs::write(
            &config_path,
            "\
Targets:
  - BMC_IP: 192.0.2.10
    RF_USERNAME: admin
    RF_PASSWORD: plain_secret
",
        )
        .expect("failed to write config");

        let mut parser = ConfigParser::new(config_path.to_string_lossy().into_owned());
        parser
            .parse_config_data()
            .await
            .expect("config should parse");

        let debug = format!("{parser:?}");
        assert!(!debug.contains("plain_secret"));
        assert!(debug.contains("XXXX"));
    }

    #[tokio::test]
    async fn parse_config_data_reads_flat_single_target_from_yaml() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let config_path = dir.path().join("config.yaml");
        std::fs::write(
            &config_path,
            "\
BMC_IP: 192.0.2.20
RF_USERNAME: admin
RF_PASSWORD: password
TUNNEL_TCP_PORT: \"2443\"
",
        )
        .expect("failed to write config");

        let mut parser = ConfigParser::new(config_path.to_string_lossy().into_owned());
        parser
            .parse_config_data()
            .await
            .expect("config should parse");

        assert_eq!(parser.targets.len(), 1);
        assert_eq!(parser.targets[0]["BMC_IP"], "192.0.2.20");
        assert_eq!(parser.targets[0]["TUNNEL_TCP_PORT"], "2443");
    }

    #[tokio::test]
    async fn parse_config_data_reads_flat_single_target_tls_options() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let config_path = dir.path().join("config.yaml");
        std::fs::write(
            &config_path,
            "\
BMC_IP: 192.0.2.20
RF_USERNAME: admin
RF_PASSWORD: password
VERIFY_TLS: true
BMC_CA_CERT: /tmp/bmc-ca.pem
",
        )
        .expect("failed to write config");

        let mut parser = ConfigParser::new(config_path.to_string_lossy().into_owned());
        parser
            .parse_config_data()
            .await
            .expect("config should parse");

        assert_eq!(parser.targets.len(), 1);
        assert_eq!(parser.targets[0]["VERIFY_TLS"], true);
        assert_eq!(parser.targets[0]["BMC_CA_CERT"], "/tmp/bmc-ca.pem");
    }

    #[tokio::test]
    async fn parse_config_data_reads_flat_single_target_ssh_host_key_options() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let config_path = dir.path().join("config.yaml");
        std::fs::write(
            &config_path,
            "\
BMC_IP: 192.0.2.20
RF_USERNAME: admin
RF_PASSWORD: password
SSH_KNOWN_HOSTS: /tmp/known_hosts
SSH_HOST_KEY_MODE: strict
",
        )
        .expect("failed to write config");

        let mut parser = ConfigParser::new(config_path.to_string_lossy().into_owned());
        parser
            .parse_config_data()
            .await
            .expect("config should parse");

        assert_eq!(parser.targets.len(), 1);
        assert_eq!(parser.targets[0]["SSH_KNOWN_HOSTS"], "/tmp/known_hosts");
        assert_eq!(parser.targets[0]["SSH_HOST_KEY_MODE"], "strict");
    }

    #[tokio::test]
    async fn parse_config_data_preserves_serde_yaml_legacy_boolean_resolution() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let config_path = dir.path().join("config.yaml");
        std::fs::write(
            &config_path,
            "\
ParallelUpdate: on
Targets:
  - BMC_IP: 192.0.2.20
    RF_USERNAME: admin
    RF_PASSWORD: password
    VERIFY_TLS: off
",
        )
        .expect("failed to write config");

        let mut parser = ConfigParser::new(config_path.to_string_lossy().into_owned());
        parser
            .parse_config_data()
            .await
            .expect("config should parse");

        assert_eq!(parser.targets.len(), 1);
        assert_eq!(parser.config_dict.as_ref().unwrap()["ParallelUpdate"], true);
        assert_eq!(parser.targets[0]["VERIFY_TLS"], false);
    }

    #[tokio::test]
    async fn parse_config_data_reads_legacy_sanitize_log_boolean() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let config_path = dir.path().join("config.yaml");
        std::fs::write(&config_path, "SANITIZE_LOG: off\n").expect("failed to write config");

        let mut parser = ConfigParser::new(config_path.to_string_lossy().into_owned());
        parser
            .parse_config_data()
            .await
            .expect("sanitize-only config should parse");

        assert!(parser.targets.is_empty());
        assert_eq!(parser.config_dict.as_ref().unwrap()["SANITIZE_LOG"], false);
    }

    #[tokio::test]
    async fn parse_config_data_rejects_empty_flat_target_fallback() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let config_path = dir.path().join("config.yaml");
        std::fs::write(
            &config_path,
            "\
TargetPlatform: GB200
ParallelUpdate: false
",
        )
        .expect("failed to write config");

        let mut parser = ConfigParser::new(config_path.to_string_lossy().into_owned());
        let err = parser
            .parse_config_data()
            .await
            .expect_err("config should reject missing target data");

        assert!(err.contains("does not contain Targets or flat target keys"));
        assert!(parser.targets.is_empty());
    }

    #[tokio::test]
    async fn parse_config_data_rejects_incomplete_flat_target_fallback() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let config_path = dir.path().join("config.yaml");
        std::fs::write(
            &config_path,
            "\
BMC_IP: 192.0.2.30
RF_USERNAME: admin
",
        )
        .expect("failed to write config");

        let mut parser = ConfigParser::new(config_path.to_string_lossy().into_owned());
        let err = parser
            .parse_config_data()
            .await
            .expect_err("config should reject incomplete target data");

        assert!(err.contains("RF_PASSWORD"));
        assert!(parser.targets.is_empty());
    }

    #[tokio::test]
    async fn parse_config_data_allows_sanitize_only_config_without_targets() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        let config_path = dir.path().join("config.yaml");
        std::fs::write(&config_path, "SANITIZE_LOG: true\n").expect("failed to write config");

        let mut parser = ConfigParser::new(config_path.to_string_lossy().into_owned());
        parser
            .parse_config_data()
            .await
            .expect("sanitize-only config should parse");

        assert!(parser.targets.is_empty());
    }
}
