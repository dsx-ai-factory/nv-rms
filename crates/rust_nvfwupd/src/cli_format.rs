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

//! CLI tracing formatter for nvfwupd's human-readable output.

use std::fmt;
use std::path::{Path, PathBuf};

use chrono::Local;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::fmt::{self as tracing_fmt, FmtContext};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

use crate::utils::Util as NvUtils;

const NVFWUPD_TARGET_PREFIX: &str = "nvfwupd";
const DEFAULT_LOG_FILE: &str = "nvfwupd_log.txt";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CliFormatDestination {
    Stdout,
    LogFile,
}

/// Formatter that renders nvfwupd tracing events in the legacy CLI shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NvfwupdCliFormat {
    destination: CliFormatDestination,
}

impl NvfwupdCliFormat {
    /// Render user-facing CLI output without timestamps.
    pub const fn stdout() -> Self {
        Self {
            destination: CliFormatDestination::Stdout,
        }
    }

    /// Render file-log output with the legacy timestamp prefix.
    pub const fn log_file() -> Self {
        Self {
            destination: CliFormatDestination::LogFile,
        }
    }

    fn render_event(&self, event: &Event<'_>) -> Option<String> {
        let metadata = event.metadata();
        if !matches!(*metadata.level(), Level::INFO | Level::DEBUG)
            || !metadata.target().starts_with(NVFWUPD_TARGET_PREFIX)
        {
            return None;
        }

        let mut fields = CliEventFields::default();
        event.record(&mut fields);
        self.render_fields(*metadata.level(), fields)
    }

    fn render_fields(&self, level: Level, fields: CliEventFields) -> Option<String> {
        if fields.message.is_empty() {
            return None;
        }

        match self.destination {
            CliFormatDestination::Stdout => {
                if fields.log_only || fields.json_mode {
                    return None;
                }
                if level == Level::DEBUG && !fields.cli_verbose {
                    return None;
                }
                let message = NvUtils::sanitize_log(&fields.message);
                Some(format!("{}{}", " ".repeat(fields.indent), message))
            }
            CliFormatDestination::LogFile => {
                let message = NvUtils::sanitize_log(&fields.message);
                let line = format!("{}{}", " ".repeat(fields.indent), message);
                Some(format!("{} {}", timestamp(), line))
            }
        }
    }
}

impl<S, N> FormatEvent<S, N> for NvfwupdCliFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        if let Some(line) = self.render_event(event) {
            writeln!(writer, "{line}")?;
        }
        Ok(())
    }
}

/// Build the stdout layer used by the nvfwupd CLI.
pub fn stdout_layer<S>() -> impl Layer<S> + Send + Sync + 'static
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    tracing_fmt::layer()
        .event_format(NvfwupdCliFormat::stdout())
        .with_ansi(false)
        .with_writer(std::io::stdout)
}

/// Build the rolling file layer used for verbose CLI log files.
pub fn rolling_file_layer<S>(log_file: impl AsRef<Path>) -> impl Layer<S> + Send + Sync + 'static
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    let (directory, file_name) = rolling_file_parts(log_file.as_ref());
    let file_appender = tracing_appender::rolling::never(directory, file_name);
    tracing_fmt::layer()
        .event_format(NvfwupdCliFormat::log_file())
        .with_ansi(false)
        .with_writer(file_appender)
}

fn rolling_file_parts(path: &Path) -> (PathBuf, PathBuf) {
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let file_name = path
        .file_name()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_LOG_FILE));
    (directory, file_name)
}

fn timestamp() -> String {
    Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct CliEventFields {
    message: String,
    indent: usize,
    log_only: bool,
    cli_verbose: bool,
    json_mode: bool,
}

impl CliEventFields {
    fn record_indent_debug(&mut self, value: &dyn fmt::Debug) {
        let raw = format!("{value:?}");
        if let Ok(indent) = raw.parse::<usize>() {
            self.indent = indent;
        }
    }
}

impl Visit for CliEventFields {
    fn record_bool(&mut self, field: &Field, value: bool) {
        match field.name() {
            "log_only" => self.log_only = value,
            "cli_verbose" => self.cli_verbose = value,
            "json_mode" => self.json_mode = value,
            _ => {}
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if field.name() == "indent" && value >= 0 {
            self.indent = value as usize;
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "indent" {
            self.indent = value as usize;
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        match field.name() {
            "message" => self.message = format!("{value:?}"),
            "indent" => self.record_indent_debug(value),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields(message: &str) -> CliEventFields {
        CliEventFields {
            message: message.to_string(),
            ..Default::default()
        }
    }

    #[test]
    fn stdout_format_matches_indent_print_shape() {
        let mut event = fields("Task Info");
        event.indent = 2;

        let line = NvfwupdCliFormat::stdout()
            .render_fields(Level::INFO, event)
            .unwrap();

        assert_eq!(line, "  Task Info");
    }

    #[test]
    fn stdout_skips_json_and_log_only_events() {
        let mut json_event = fields("json hidden");
        json_event.json_mode = true;
        assert_eq!(
            NvfwupdCliFormat::stdout().render_fields(Level::INFO, json_event),
            None
        );

        let mut log_only_event = fields("file only");
        log_only_event.log_only = true;
        assert_eq!(
            NvfwupdCliFormat::stdout().render_fields(Level::INFO, log_only_event),
            None
        );
    }

    #[test]
    fn stdout_debug_requires_cli_verbose_field() {
        assert_eq!(
            NvfwupdCliFormat::stdout().render_fields(Level::DEBUG, fields("hidden")),
            None
        );

        let mut event = fields("visible");
        event.cli_verbose = true;
        assert_eq!(
            NvfwupdCliFormat::stdout().render_fields(Level::DEBUG, event),
            Some("visible".to_string())
        );
    }

    #[test]
    fn render_fields_redacts_passwords() {
        let line = NvfwupdCliFormat::stdout()
            .render_fields(Level::INFO, fields("target password=plain_secret"))
            .unwrap();

        assert!(!line.contains("plain_secret"));
        assert!(line.contains("XXXX"));
    }

    #[test]
    fn log_file_format_includes_timestamp_prefix_and_log_only_events() {
        let mut event = fields("file only");
        event.log_only = true;
        event.indent = 1;

        let line = NvfwupdCliFormat::log_file()
            .render_fields(Level::INFO, event)
            .unwrap();

        assert_eq!(&line[19..], "  file only");
        assert_eq!(line.as_bytes()[4], b'-');
        assert_eq!(line.as_bytes()[7], b'-');
        assert_eq!(line.as_bytes()[10], b' ');
        assert_eq!(line.as_bytes()[13], b':');
        assert_eq!(line.as_bytes()[16], b':');
    }

    #[test]
    fn rolling_file_parts_split_directory_and_name() {
        let (directory, file_name) = rolling_file_parts(Path::new("/tmp/nvfwupd.log"));
        assert_eq!(directory, PathBuf::from("/tmp"));
        assert_eq!(file_name, PathBuf::from("nvfwupd.log"));

        let (directory, file_name) = rolling_file_parts(Path::new("nvfwupd.log"));
        assert_eq!(directory, PathBuf::from("."));
        assert_eq!(file_name, PathBuf::from("nvfwupd.log"));
    }
}
