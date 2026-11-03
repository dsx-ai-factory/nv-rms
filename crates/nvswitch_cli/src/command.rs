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

use crate::error::{InvalidCommand, Result};

/// Stdout marker echoed by [`ShellCommand::ConfirmFile`] when the file exists.
pub(crate) const FILE_CONFIRMATION: &str = "ok";

/// Reject anything that could introduce whitespace or shell metacharacters.
///
/// A leading `-` is also rejected, mirroring [`RemotePath::new`]: `NvCommand`
/// renders its arguments space-joined and unquoted, so a dash-prefixed segment
/// would reach the `nv` CLI as a bare, option-shaped token and be parsed as a
/// flag rather than a literal value. (A `-` elsewhere in the token, e.g.
/// `gnmi-server`, is fine.)
fn validate_identifier(kind: &'static str, value: &str) -> Result<()> {
    (!value.is_empty()
        && !value.starts_with('-')
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'))
    .then_some(())
    .ok_or_else(|| InvalidCommand::Identifier {
        kind,
        value: value.to_owned(),
    })
}

/// A validated NVOS configuration path, such as `system gnmi-server mtls`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NvPath {
    segments: Vec<String>,
}

impl NvPath {
    /// Build a path from ordered segments.
    pub fn new<I, S>(segments: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let segments = segments
            .into_iter()
            .map(|segment| {
                let segment = segment.as_ref();
                validate_identifier("NVOS path segment", segment)?;
                Ok(segment.to_owned())
            })
            .collect::<Result<Vec<_>>>()?;

        if segments.is_empty() {
            return Err(InvalidCommand::EmptyPath);
        }

        Ok(Self { segments })
    }
}

impl std::fmt::Display for NvPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut segments = self.segments.iter();

        if let Some(first) = segments.next() {
            write!(f, "{first}")?;
        }

        for segment in segments {
            write!(f, " {segment}")?;
        }

        Ok(())
    }
}

/// A validated remote filesystem path.
#[derive(Clone, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(Debug))]
pub struct RemotePath {
    path: String,
}

impl RemotePath {
    /// Build a remote path.
    ///
    /// Spaces and most punctuation are allowed because the path is quoted at
    /// render time. Four shapes are rejected:
    ///
    /// # Errors
    ///
    /// Returns [`InvalidCommand::RemotePath`] when the path is empty, starts
    /// with `-`, or contains a NUL or line break.
    pub fn new(path: impl Into<String>) -> Result<Self> {
        let path = path.into();

        let reason = if path.is_empty() {
            Some("path must not be empty")
        } else if path.contains('\0') {
            Some("path must not contain NUL")
        } else if path.contains('\n') || path.contains('\r') {
            Some("path must not contain a line break")
        } else if path.starts_with('-') {
            Some("path must not start with '-'; it would parse as a command option")
        } else {
            None
        };

        match reason {
            Some(reason) => Err(InvalidCommand::RemotePath {
                value: path,
                reason,
            }),
            None => Ok(Self { path }),
        }
    }

    /// The raw, unquoted path, for human-readable error and log messages.
    ///
    /// This is NOT safe to interpolate into a command line; use
    /// [`quoted`](Self::quoted) for that. It exists so a message can show the
    /// plain path the caller passed in, matching how the rest of RMS reports
    /// remote paths.
    pub(crate) fn as_str(&self) -> &str {
        &self.path
    }

    /// Return the path quoted for a POSIX shell.
    pub(crate) fn quoted(&self) -> String {
        let mut quoted = String::with_capacity(self.path.len() + 2);
        quoted.push('\'');

        for c in self.path.chars() {
            if c == '\'' {
                quoted.push_str("'\\''");
            } else {
                quoted.push(c);
            }
        }

        quoted.push('\'');
        quoted
    }
}

/// A POSIX permission mode -- the low 12 bits (`setuid`/`setgid`/`sticky` plus
/// the owner/group/other `rwx` triples) -- rendered in octal for `chmod` /
/// `install -m`.
///
/// A `FileMode` can only be assembled through [`FileMode::builder`], one
/// permission bit at a time. There is deliberately no constructor that takes a
/// raw integer, so an out-of-range or otherwise malformed mode is
/// unrepresentable rather than caught at runtime.
///
/// # Examples
///
/// ```
/// use nvswitch_cli::FileMode;
///
/// // rwxr-x--x
/// let mode = FileMode::builder()
///     .owner_read()
///     .owner_write()
///     .owner_execute()
///     .group_read()
///     .group_execute()
///     .other_execute()
///     .build();
/// assert_eq!(mode.to_string(), "751");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FileMode(u32);

impl FileMode {
    /// Start building a mode with no bits set (`0o000`).
    ///
    /// Chain the `owner_*` / `group_*` / `other_*` bit methods (and the optional
    /// `setuid` / `setgid` / `sticky` methods), then finish with
    /// [`FileModeBuilder::build`]. Every builder method is `const`, so a mode can
    /// be defined in a `const` item.
    pub const fn builder() -> FileModeBuilder {
        FileModeBuilder { bits: 0 }
    }
}

impl std::fmt::Display for FileMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:o}", self.0)
    }
}

/// Fluent builder for [`FileMode`] that composes a permission mode a single bit
/// at a time.
///
/// Each method only *sets* one well-defined permission bit, so every value the
/// builder can yield lies within the `0o7777` permission space -- an invalid
/// mode cannot be constructed, which removes the need for any runtime range
/// check. All methods are `const` and take and return `self`, so a mode can be
/// assembled fluently, including in a `const` context.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct FileModeBuilder {
    bits: u32,
}

impl FileModeBuilder {
    /// Owner read (`0o400`).
    pub const fn owner_read(mut self) -> Self {
        self.bits |= 0o400;
        self
    }

    /// Owner write (`0o200`).
    pub const fn owner_write(mut self) -> Self {
        self.bits |= 0o200;
        self
    }

    /// Owner execute / directory search (`0o100`).
    pub const fn owner_execute(mut self) -> Self {
        self.bits |= 0o100;
        self
    }

    /// Group read (`0o040`).
    pub const fn group_read(mut self) -> Self {
        self.bits |= 0o040;
        self
    }

    /// Group write (`0o020`).
    pub const fn group_write(mut self) -> Self {
        self.bits |= 0o020;
        self
    }

    /// Group execute / directory search (`0o010`).
    pub const fn group_execute(mut self) -> Self {
        self.bits |= 0o010;
        self
    }

    /// Other read (`0o004`).
    pub const fn other_read(mut self) -> Self {
        self.bits |= 0o004;
        self
    }

    /// Other write (`0o002`).
    pub const fn other_write(mut self) -> Self {
        self.bits |= 0o002;
        self
    }

    /// Other execute / directory search (`0o001`).
    pub const fn other_execute(mut self) -> Self {
        self.bits |= 0o001;
        self
    }

    /// Set-user-ID on execution (`0o4000`).
    pub const fn setuid(mut self) -> Self {
        self.bits |= 0o4000;
        self
    }

    /// Set-group-ID on execution (`0o2000`).
    pub const fn setgid(mut self) -> Self {
        self.bits |= 0o2000;
        self
    }

    /// Restricted-deletion / sticky bit (`0o1000`).
    pub const fn sticky(mut self) -> Self {
        self.bits |= 0o1000;
        self
    }

    /// Finish building, yielding the composed [`FileMode`].
    pub const fn build(self) -> FileMode {
        FileMode(self.bits)
    }
}

/// A validated NMX cluster application name.
///
/// # Examples
///
/// ```
/// use nvswitch_cli::ClusterApp;
///
/// assert_eq!(ClusterApp::nmx_controller().as_str(), "nmx-controller");
/// assert!(ClusterApp::new("nmx telemetry").is_err());
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClusterApp {
    name: String,
}

impl ClusterApp {
    /// NMX Controller, the scale-up fabric manager application.
    pub const NMX_CONTROLLER: &'static str = "nmx-controller";

    /// NMX Telemetry, the scale-up fabric telemetry application.
    pub const NMX_TELEMETRY: &'static str = "nmx-telemetry";

    /// Build a cluster app name.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidCommand::Identifier`] when `name` is not a valid NVOS
    /// identifier.
    pub fn new(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        validate_identifier("cluster app", &name)?;

        Ok(Self { name })
    }

    /// The NMX Controller application.
    pub fn nmx_controller() -> Self {
        Self {
            name: Self::NMX_CONTROLLER.to_owned(),
        }
    }

    /// The NMX Telemetry application.
    pub fn nmx_telemetry() -> Self {
        Self {
            name: Self::NMX_TELEMETRY.to_owned(),
        }
    }

    /// The application name as it appears in a command line.
    pub fn as_str(&self) -> &str {
        &self.name
    }
}

impl std::fmt::Display for ClusterApp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.name)
    }
}

// ═══════════════════════════════════════════════════════════════════
//  `nv` CLI commands
// ═══════════════════════════════════════════════════════════════════

/// A `nv config` subcommand.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NvConfigOperation {
    /// Apply the pending configuration, prompting for confirmation.
    Apply,

    /// Apply the pending configuration without prompting.
    ///
    /// Every non-interactive caller wants this form; the prompting variant
    /// blocks forever on a channel with no TTY.
    ApplyAssumeYes,

    /// Persist the running configuration so it survives a reboot.
    Save,

    /// Discard the pending configuration.
    Detach,
}

impl NvConfigOperation {
    /// Arguments following `nv config`.
    fn args(self) -> &'static [&'static str] {
        match self {
            Self::Apply => &["apply"],
            Self::ApplyAssumeYes => &["apply", "--assume-yes"],
            Self::Save => &["save"],
            Self::Detach => &["detach"],
        }
    }
}

/// A `nv action` verb.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NvActionVerb {
    /// Change a managed object's configuration.
    Update,

    /// Return a managed object to its default configuration.
    Restore,

    /// Start a managed object.
    Start,

    /// Stop a managed object.
    Stop,

    /// Import external material, such as a certificate.
    Import,

    /// Install staged material, such as a firmware image.
    Install,

    /// Remove installed material.
    Uninstall,

    /// Fetch material from a remote URI onto the switch.
    Fetch,

    /// Delete a managed object.
    Delete,
}

impl NvActionVerb {
    /// The verb as it appears after `nv action`.
    fn as_str(self) -> &'static str {
        match self {
            Self::Update => "update",
            Self::Restore => "restore",
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Import => "import",
            Self::Install => "install",
            Self::Uninstall => "uninstall",
            Self::Fetch => "fetch",
            Self::Delete => "delete",
        }
    }
}

/// Encryption applied to a cluster application's manager channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ManagerEncryption {
    /// Mutual TLS, set with `nv action update … manager encryption mtls`.
    Mtls,

    /// The NVOS default, restored with `nv action restore … manager encryption`.
    Default,
}

/// An NVOS service whose mTLS binding the `nv` CLI can clear.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum MtlsService {
    /// The NVUE REST API.
    NvueApi,

    /// The gNMI telemetry interface.
    GnmiServer,

    /// A cluster application's manager channel.
    ClusterApp(ClusterApp),
}

impl MtlsService {
    /// The NVOS object this service's mTLS binding lives on, named the way
    /// NVOS spells it. Used to attribute a step outcome to a service without
    /// quoting the command that acted on it.
    ///
    /// ```
    /// use nvswitch_cli::{ClusterApp, MtlsService};
    ///
    /// assert_eq!(MtlsService::GnmiServer.label(), "gnmi-server");
    /// assert_eq!(
    ///     MtlsService::ClusterApp(ClusterApp::nmx_telemetry()).label(),
    ///     "nmx-telemetry",
    /// );
    /// ```
    pub fn label(&self) -> &str {
        match self {
            Self::NvueApi => "nvue-api",
            Self::GnmiServer => "gnmi-server",
            Self::ClusterApp(app) => app.as_str(),
        }
    }
}

/// One `nv` CLI invocation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NvCommand {
    /// Arguments following `nv`, already validated.
    args: Vec<String>,
}

impl NvCommand {
    /// Build a command from validated argument fragments.
    fn from_args<I, S>(args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            args: args.into_iter().map(Into::into).collect(),
        }
    }

    // ── Generic constructors ────────────────────────────────────────

    /// `nv unset <path>`
    pub fn unset(path: &NvPath) -> Self {
        Self::from_args(["unset".to_owned(), path.to_string()])
    }

    /// `nv config <operation>`
    pub fn config(operation: NvConfigOperation) -> Self {
        Self::from_args(
            ["config"]
                .into_iter()
                .chain(operation.args().iter().copied()),
        )
    }

    /// `nv action <verb> <path>`
    pub fn action(verb: NvActionVerb, path: &NvPath) -> Self {
        Self::from_args([
            "action".to_owned(),
            verb.as_str().to_owned(),
            path.to_string(),
        ])
    }

    // ── Named commands ──────────────────────────────────────────────

    /// Force a full NVOS factory reset as `admin` without confirmation.
    ///
    /// The command initiates a reboot, which may close the SSH connection.
    ///
    /// ```
    /// use nvswitch_cli::{NvCommand, SwitchCommand};
    ///
    /// let command = SwitchCommand::Nv(NvCommand::factory_reset());
    /// assert_eq!(command.render(), "nv action reset system factory-default force");
    /// ```
    pub fn factory_reset() -> Self {
        Self::from_args(["action", "reset", "system", "factory-default", "force"])
    }

    /// The command that clears `service`'s mTLS binding.
    pub fn unset_mtls(service: &MtlsService) -> Result<Self> {
        match service {
            MtlsService::NvueApi => Ok(Self::unset(&NvPath::new([
                "system",
                "api",
                "mtls",
                "ca-certificate",
            ])?)),
            MtlsService::GnmiServer => Self::unset_gnmi_server_mtls_certificate(),
            MtlsService::ClusterApp(app) => {
                Self::cluster_app_manager_encryption(app, ManagerEncryption::Default)
            }
        }
    }

    /// `nv unset system gnmi-server mtls certificate`
    pub fn unset_gnmi_server_mtls_certificate() -> Result<Self> {
        Ok(Self::unset(&NvPath::new([
            "system",
            "gnmi-server",
            "mtls",
            "certificate",
        ])?))
    }

    /// `nv unset system gnmi-server mtls ca-certificate`
    pub fn unset_gnmi_server_mtls_ca_certificate() -> Result<Self> {
        Ok(Self::unset(&NvPath::new([
            "system",
            "gnmi-server",
            "mtls",
            "ca-certificate",
        ])?))
    }

    /// Set or restore a cluster application's manager encryption.
    pub fn cluster_app_manager_encryption(
        app: &ClusterApp,
        encryption: ManagerEncryption,
    ) -> Result<Self> {
        let path = NvPath::new(["cluster", "apps", app.as_str(), "manager", "encryption"])?;

        let command = match encryption {
            ManagerEncryption::Mtls => {
                let mut command = Self::action(NvActionVerb::Update, &path);
                command.args.push("mtls".to_owned());
                command
            }
            ManagerEncryption::Default => Self::action(NvActionVerb::Restore, &path),
        };

        Ok(command)
    }

    /// Render the full command line.
    fn render(&self) -> String {
        std::iter::once("nv")
            .chain(self.args.iter().map(String::as_str))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

impl std::fmt::Display for NvCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.render())
    }
}

// ═══════════════════════════════════════════════════════════════════
//  POSIX helper commands
// ═══════════════════════════════════════════════════════════════════

/// A POSIX helper run over the same session as the `nv` CLI.
#[derive(Clone, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(Debug))]
pub enum ShellCommand {
    /// `install -d -m <mode> <path>`
    ///
    /// Creates the directory with exactly `mode`, unaffected by the remote
    /// umask. Prefer this over `mkdir` whenever the permissions matter.
    CreateDirectory {
        /// Directory to create.
        path: RemotePath,

        /// Mode applied to the directory.
        mode: FileMode,
    },

    /// `mkdir -p <path>`
    ///
    /// Creates the directory and any missing parents, leaving permissions to
    /// the remote umask.
    CreateDirectoryTree {
        /// Directory to create.
        path: RemotePath,
    },

    /// `test -f <path> && echo ok`
    ///
    /// Exits non-zero when the file is absent *and* prints nothing, so a
    /// missing file is caught whether or not the transport surfaces exit
    /// status.
    ConfirmFile {
        /// File expected to exist.
        path: RemotePath,
    },

    /// `chmod <mode> <path>`
    SetPermissions {
        /// File to modify.
        path: RemotePath,

        /// Mode to apply.
        mode: FileMode,
    },

    /// `rm -f <path>`
    RemoveFile {
        /// File to remove.
        path: RemotePath,
    },

    /// `rm -rf <path>`
    ///
    /// Recursively removes `path` and everything under it, succeeding when it
    /// is already absent. Intended for scratch/staging directories the caller
    /// created and fully controls. Because it is recursive, callers must never
    /// pass an attacker-influenced path; the [`RemotePath`] construction rules
    /// (no empty, `-`, NUL, or newline) are a guardrail, not a full defense.
    RemoveDirectory {
        /// Directory to remove, along with everything under it.
        path: RemotePath,
    },
}

impl ShellCommand {
    /// Render the full command line, with every path shell-quoted.
    fn render(&self) -> String {
        match self {
            Self::CreateDirectory { path, mode } => {
                format!("install -d -m {mode} {}", path.quoted())
            }
            Self::CreateDirectoryTree { path } => format!("mkdir -p {}", path.quoted()),
            Self::ConfirmFile { path } => {
                format!("test -f {} && echo {FILE_CONFIRMATION}", path.quoted())
            }
            Self::SetPermissions { path, mode } => format!("chmod {mode} {}", path.quoted()),
            Self::RemoveFile { path } => format!("rm -f {}", path.quoted()),
            Self::RemoveDirectory { path } => format!("rm -rf {}", path.quoted()),
        }
    }
}

impl std::fmt::Display for ShellCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.render())
    }
}

// ═══════════════════════════════════════════════════════════════════
//  Unified command
// ═══════════════════════════════════════════════════════════════════

/// Any command the handler can run on a switch.
#[derive(Clone, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(Debug))]
pub enum SwitchCommand {
    /// An NVOS `nv` CLI invocation.
    Nv(NvCommand),

    /// A POSIX helper.
    Shell(ShellCommand),
}

impl SwitchCommand {
    /// Render the full command line sent to the transport.
    pub fn render(&self) -> String {
        match self {
            Self::Nv(command) => command.render(),
            Self::Shell(command) => command.render(),
        }
    }

    /// Render the command line for logs.
    ///
    /// Currently identical to [`render`](Self::render). If a command variant
    /// that carries sensitive material (a key, a password, a token) is added,
    /// this method must be updated to mask that argument before any logging
    /// call-site is modified.
    pub fn render_masked(&self) -> String {
        match self {
            Self::Nv(command) => command.render(),
            Self::Shell(command) => command.render(),
        }
    }

    /// Output that must appear on stdout for the command to count as success,
    /// beyond a zero exit status.
    pub(crate) fn required_output(&self) -> Option<&'static str> {
        match self {
            Self::Shell(ShellCommand::ConfirmFile { .. }) => Some(FILE_CONFIRMATION),
            _ => None,
        }
    }
}

impl From<NvCommand> for SwitchCommand {
    fn from(command: NvCommand) -> Self {
        Self::Nv(command)
    }
}

impl From<ShellCommand> for SwitchCommand {
    fn from(command: ShellCommand) -> Self {
        Self::Shell(command)
    }
}

impl std::fmt::Display for SwitchCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.render())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nv_path_rejects_shell_metacharacters() {
        for segment in ["gnmi server", "a;rm -rf /", "$(id)", "", "a'b"] {
            assert!(
                NvPath::new(["system", segment]).is_err(),
                "expected {segment:?} to be rejected"
            );
        }
    }

    #[test]
    fn nv_path_renders_segments_in_order() {
        let path = NvPath::new(["system", "api", "mtls", "ca-certificate"]).unwrap();

        assert_eq!(path.to_string(), "system api mtls ca-certificate");
    }

    #[test]
    fn nv_path_requires_at_least_one_segment() {
        let empty: [&str; 0] = [];

        assert_eq!(NvPath::new(empty).unwrap_err(), InvalidCommand::EmptyPath);
    }

    #[test]
    fn config_operations_render_expected_commands() {
        let cases = [
            (NvConfigOperation::Apply, "nv config apply"),
            (
                NvConfigOperation::ApplyAssumeYes,
                "nv config apply --assume-yes",
            ),
            (NvConfigOperation::Save, "nv config save"),
            (NvConfigOperation::Detach, "nv config detach"),
        ];

        for (operation, expected) in cases {
            assert_eq!(NvCommand::config(operation).to_string(), expected);
        }
    }

    #[test]
    fn unset_mtls_renders_the_command_for_each_service() -> Result<()> {
        let cases = [
            (
                MtlsService::NvueApi,
                "nv unset system api mtls ca-certificate",
            ),
            (
                MtlsService::GnmiServer,
                "nv unset system gnmi-server mtls certificate",
            ),
            (
                MtlsService::ClusterApp(ClusterApp::nmx_controller()),
                "nv action restore cluster apps nmx-controller manager encryption",
            ),
            (
                MtlsService::ClusterApp(ClusterApp::nmx_telemetry()),
                "nv action restore cluster apps nmx-telemetry manager encryption",
            ),
        ];

        for (service, expected) in cases {
            assert_eq!(NvCommand::unset_mtls(&service)?.to_string(), expected);
        }

        Ok(())
    }

    /// Every named command builds its path through `NvPath::new`, so a typo in
    /// one of the hardcoded segments surfaces as an error here rather than as a
    /// malformed command against a switch.
    #[test]
    fn named_command_paths_pass_validation() {
        let app = ClusterApp::nmx_controller();

        let commands = [
            NvCommand::unset_mtls(&MtlsService::NvueApi),
            NvCommand::unset_mtls(&MtlsService::GnmiServer),
            NvCommand::unset_mtls(&MtlsService::ClusterApp(app.clone())),
            NvCommand::unset_gnmi_server_mtls_certificate(),
            NvCommand::unset_gnmi_server_mtls_ca_certificate(),
            NvCommand::cluster_app_manager_encryption(&app, ManagerEncryption::Mtls),
            NvCommand::cluster_app_manager_encryption(&app, ManagerEncryption::Default),
        ];

        for command in commands {
            let Ok(command) = command else {
                panic!("named command failed validation: {command:?}");
            };

            let rendered = command.to_string();

            assert!(rendered.starts_with("nv "), "unexpected: {rendered}");
            assert!(!rendered.contains("  "), "empty segment in: {rendered}");
        }
    }

    #[test]
    fn gnmi_fallback_uses_the_ca_certificate_spelling() -> Result<()> {
        assert_eq!(
            NvCommand::unset_gnmi_server_mtls_ca_certificate()?.to_string(),
            "nv unset system gnmi-server mtls ca-certificate"
        );

        Ok(())
    }

    #[test]
    fn manager_encryption_uses_different_verbs_per_mode() -> Result<()> {
        let app = ClusterApp::nmx_controller();

        assert_eq!(
            NvCommand::cluster_app_manager_encryption(&app, ManagerEncryption::Mtls)?.to_string(),
            "nv action update cluster apps nmx-controller manager encryption mtls"
        );

        assert_eq!(
            NvCommand::cluster_app_manager_encryption(&app, ManagerEncryption::Default)?
                .to_string(),
            "nv action restore cluster apps nmx-controller manager encryption"
        );

        Ok(())
    }

    #[test]
    fn cluster_app_rejects_non_identifier_names() {
        assert!(ClusterApp::new("nmx controller").is_err());
        assert!(ClusterApp::new("").is_err());
        assert_eq!(
            ClusterApp::new("nmx-telemetry").unwrap().as_str(),
            "nmx-telemetry"
        );
    }

    /// A leading `-` must be rejected for the same reason `RemotePath` rejects
    /// it: `NvCommand` renders segments space-joined and unquoted, so a
    /// dash-prefixed identifier would reach the `nv` CLI as a bare option token
    /// rather than a literal value. A dash elsewhere in the token stays legal.
    #[test]
    fn identifiers_reject_a_leading_dash() {
        for name in ["-x", "-rf", "--app", "-"] {
            assert!(
                ClusterApp::new(name).is_err(),
                "expected ClusterApp::new({name:?}) to be rejected"
            );
            assert!(
                NvPath::new(["system", name]).is_err(),
                "expected NvPath segment {name:?} to be rejected"
            );
        }

        // A dash anywhere but the first character remains an ordinary
        // identifier character.
        assert!(ClusterApp::new("nmx-controller").is_ok());
        assert!(NvPath::new(["system", "gnmi-server"]).is_ok());
    }

    #[test]
    fn show_and_action_render_generic_paths() {
        let path = NvPath::new(["cluster", "apps"]).unwrap();

        assert_eq!(
            NvCommand::action(NvActionVerb::Start, &path).to_string(),
            "nv action start cluster apps"
        );
    }

    #[test]
    fn factory_reset_renders_as_an_nv_command() {
        let command = SwitchCommand::Nv(NvCommand::factory_reset());

        assert_eq!(
            command.render(),
            "nv action reset system factory-default force"
        );

        assert_eq!(command.render_masked(), command.render());
    }

    #[test]
    fn shell_commands_quote_paths() {
        let path = RemotePath::new("/home/admin/my certs/ca.pem").unwrap();
        let mode = FileMode::builder().owner_read().build();

        let cases = [
            (
                ShellCommand::CreateDirectory {
                    path: path.clone(),
                    mode: FileMode::builder()
                        .owner_read()
                        .owner_write()
                        .owner_execute()
                        .group_read()
                        .group_execute()
                        .other_execute()
                        .build(),
                },
                "install -d -m 751 '/home/admin/my certs/ca.pem'",
            ),
            (
                ShellCommand::CreateDirectoryTree { path: path.clone() },
                "mkdir -p '/home/admin/my certs/ca.pem'",
            ),
            (
                ShellCommand::ConfirmFile { path: path.clone() },
                "test -f '/home/admin/my certs/ca.pem' && echo ok",
            ),
            (
                ShellCommand::SetPermissions {
                    path: path.clone(),
                    mode,
                },
                "chmod 400 '/home/admin/my certs/ca.pem'",
            ),
            (
                ShellCommand::RemoveFile { path: path.clone() },
                "rm -f '/home/admin/my certs/ca.pem'",
            ),
            (
                ShellCommand::RemoveDirectory { path },
                "rm -rf '/home/admin/my certs/ca.pem'",
            ),
        ];

        for (command, expected) in cases {
            assert_eq!(command.to_string(), expected);
        }
    }

    #[test]
    fn shell_quote_escapes_embedded_single_quotes() {
        let path = RemotePath::new("/tmp/it's here").unwrap();

        assert_eq!(
            ShellCommand::RemoveFile { path }.to_string(),
            r"rm -f '/tmp/it'\''s here'"
        );
    }

    #[test]
    fn remote_path_rejects_command_breaking_characters() {
        assert!(RemotePath::new("").is_err());
        assert!(RemotePath::new("/tmp/a\0b").is_err());
        assert!(RemotePath::new("/tmp/a\nrm -rf /").is_err());
        assert!(RemotePath::new("/tmp/a\rb").is_err());
    }

    /// Quoting stops the *shell* from splitting a word; it does not stop the
    /// receiving program from parsing it as options. `rm -f '-rf'` still hands
    /// `rm` a flag, so option-shaped paths are rejected outright.
    #[test]
    fn remote_path_rejects_option_shaped_paths() {
        for path in ["-rf", "-f", "--no-preserve-root", "-"] {
            assert!(
                RemotePath::new(path).is_err(),
                "expected {path:?} to be rejected"
            );
        }

        // A dash anywhere but the first character is an ordinary path
        // character.
        assert!(RemotePath::new("/tmp/-rf").is_ok());
        assert!(RemotePath::new("/host/fw-images/bmc").is_ok());
    }

    #[test]
    fn only_file_confirmation_requires_stdout() {
        let path = RemotePath::new("/tmp/a").unwrap();

        assert_eq!(
            SwitchCommand::from(ShellCommand::ConfirmFile { path: path.clone() }).required_output(),
            Some("ok")
        );
        assert_eq!(
            SwitchCommand::from(ShellCommand::RemoveFile { path }).required_output(),
            None
        );
        assert_eq!(
            SwitchCommand::from(NvCommand::config(NvConfigOperation::Save)).required_output(),
            None
        );
    }

    #[test]
    fn file_mode_builder_composes_and_renders_as_octal() {
        // rwxr-x--x
        assert_eq!(
            FileMode::builder()
                .owner_read()
                .owner_write()
                .owner_execute()
                .group_read()
                .group_execute()
                .other_execute()
                .build()
                .to_string(),
            "751"
        );
        // rw-r--r--
        assert_eq!(
            FileMode::builder()
                .owner_read()
                .owner_write()
                .group_read()
                .other_read()
                .build()
                .to_string(),
            "644"
        );
        // r--------
        assert_eq!(FileMode::builder().owner_read().build().to_string(), "400");
    }

    #[test]
    fn file_mode_builder_covers_the_full_permission_space() {
        // No bits set renders as "0"; every bit set is the 0o7777 ceiling, so the
        // builder can never produce a value outside the permission space.
        assert_eq!(FileMode::builder().build().to_string(), "0");
        assert_eq!(
            FileMode::builder()
                .setuid()
                .setgid()
                .sticky()
                .owner_read()
                .owner_write()
                .owner_execute()
                .group_read()
                .group_write()
                .group_execute()
                .other_read()
                .other_write()
                .other_execute()
                .build()
                .to_string(),
            "7777"
        );
    }

    #[test]
    fn file_mode_builder_is_usable_in_const_context() {
        const PRIVATE_KEY: FileMode = FileMode::builder().owner_read().build();
        assert_eq!(PRIVATE_KEY.to_string(), "400");
    }
}
