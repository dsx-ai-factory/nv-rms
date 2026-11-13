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

//! The single entry point for running NVOS commands on a switch.

use std::time::Duration;

use crate::command::{FileMode, RemotePath, ShellCommand, SwitchCommand};
use crate::flow::{BenignOutcome, BenignReason, CommandFlow, FlowStep, StepSubject};
use crate::transport::{CommandFailure, NvSwitchTransport};

/// Timeouts applied to every command a handler runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HandlerConfig {
    /// Per-command timeout handed to the transport.
    pub command_timeout: Duration,
}

impl HandlerConfig {
    /// Timeout applied when a caller does not choose one.
    pub const DEFAULT_COMMAND_TIMEOUT: Duration = Duration::from_secs(30);
}

impl Default for HandlerConfig {
    fn default() -> Self {
        Self {
            command_timeout: Self::DEFAULT_COMMAND_TIMEOUT,
        }
    }
}

/// How one [`FlowStep`] of a [`CommandFlow`] turned out.
///
/// Deliberately carries no rendered command line. A step is identified by its
/// [`StepSubject`], so a caller can report *what* changed - and attribute a
/// partial failure - without handling command text that may be logged, stored,
/// or returned to an API client.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StepStatus {
    /// The step's command succeeded. `used_fallback` is true when the primary
    /// command form failed and the alternate spelling is what actually ran.
    Succeeded { used_fallback: bool },

    /// The step failed, but the failure means the switch is already in the
    /// desired state. See [`BenignReason`].
    Benign { reason: BenignReason },

    /// The step failed and aborted the flow. `message` is the switch's own
    /// error text, with no command line wrapped around it.
    Failed { message: String },

    /// The step never ran: an earlier step aborted the flow, or the flow
    /// completed early because the remaining steps became unnecessary.
    NotAttempted,
}

/// The outcome of one [`FlowStep`], paired with what it acted on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepReport {
    subject: Option<StepSubject>,
    status: StepStatus,
}

impl StepReport {
    /// What the step acted on, or `None` for an unlabelled step.
    pub fn subject(&self) -> Option<&StepSubject> {
        self.subject.as_ref()
    }

    /// How the step turned out.
    pub fn status(&self) -> &StepStatus {
        &self.status
    }
}

/// Per-step outcomes for one [`CommandFlow`] run, in flow order.
///
/// A report always has one entry per step in the flow, including on the
/// failure path: steps after an abort are reported [`StepStatus::NotAttempted`]
/// rather than omitted, so a caller can tell "not done" apart from "unknown".
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct FlowReport {
    steps: Vec<StepReport>,
    interrupted: bool,
}

impl FlowReport {
    /// Every step's outcome, in the order the flow declared them.
    pub fn steps(&self) -> &[StepReport] {
        &self.steps
    }

    /// Whether the caller stopped the run part-way through.
    ///
    /// A run cut short by [`NvSwitchCliHandler::run_flow_while`] and one that
    /// ended on its own both leave trailing steps
    /// [`StepStatus::NotAttempted`]; only this tells them apart.
    pub fn interrupted(&self) -> bool {
        self.interrupted
    }

    /// Whether the report has no steps, i.e. the flow was empty.
    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// The first step that aborted the flow, if any.
    pub fn failed_step(&self) -> Option<&StepReport> {
        self.steps
            .iter()
            .find(|step| matches!(step.status, StepStatus::Failed { .. }))
    }
}

/// A flow that aborted, carrying both the transport's error and the partial
/// report describing how far the flow got.
///
/// The report is the point: without it, a caller can only say the flow failed,
/// not which steps had already taken effect on the switch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlowFailure<F> {
    /// The failure the transport (or the step policy) produced.
    pub failure: F,

    /// Outcomes for every step, including those never attempted.
    pub report: FlowReport,
}

/// Runs NVOS `nv` CLI commands, POSIX helpers, and multi-step flows on one
/// switch.
pub struct NvSwitchCliHandler<T> {
    transport: T,
    config: HandlerConfig,
}

impl<T: NvSwitchTransport> NvSwitchCliHandler<T> {
    /// Build a handler with the default configuration.
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            config: HandlerConfig::default(),
        }
    }

    /// Build a handler with an explicit timeout.
    pub fn with_config(transport: T, config: HandlerConfig) -> Self {
        Self { transport, config }
    }

    /// The configuration in force.
    pub fn config(&self) -> HandlerConfig {
        self.config
    }

    /// The underlying transport.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    // ── Single commands ─────────────────────────────────────────────

    /// Run one command and return its standard output.
    pub async fn run(
        &self,
        command: impl Into<SwitchCommand>,
    ) -> Result<String, <T as NvSwitchTransport>::Failure> {
        let command = command.into();
        self.execute_checked(&command).await
    }

    /// Create `path` with exactly `mode`, independent of the remote umask.
    pub async fn create_directory(
        &self,
        path: &RemotePath,
        mode: FileMode,
    ) -> Result<(), <T as NvSwitchTransport>::Failure> {
        self.run(ShellCommand::CreateDirectory {
            path: path.clone(),
            mode,
        })
        .await
        .map(drop)
    }

    /// Create `path` and any missing parents.
    pub async fn create_directory_tree(
        &self,
        path: &RemotePath,
    ) -> Result<(), <T as NvSwitchTransport>::Failure> {
        self.run(ShellCommand::CreateDirectoryTree { path: path.clone() })
            .await
            .map(drop)
    }

    /// Set `path`'s permissions.
    pub async fn set_permissions(
        &self,
        path: &RemotePath,
        mode: FileMode,
    ) -> Result<(), <T as NvSwitchTransport>::Failure> {
        self.run(ShellCommand::SetPermissions {
            path: path.clone(),
            mode,
        })
        .await
        .map(drop)
    }

    /// Remove `path`, succeeding when it is already absent.
    pub async fn remove_file(
        &self,
        path: &RemotePath,
    ) -> Result<(), <T as NvSwitchTransport>::Failure> {
        self.run(ShellCommand::RemoveFile { path: path.clone() })
            .await
            .map(drop)
    }

    /// Recursively remove `path` and its contents, succeeding when it is
    /// already absent. Use for scratch/staging directories the caller created;
    /// see [`ShellCommand::RemoveDirectory`] for the safety expectations.
    pub async fn remove_directory(
        &self,
        path: &RemotePath,
    ) -> Result<(), <T as NvSwitchTransport>::Failure> {
        self.run(ShellCommand::RemoveDirectory { path: path.clone() })
            .await
            .map(drop)
    }

    /// Confirm `path` is a regular file on the switch.
    pub async fn confirm_file(
        &self,
        path: &RemotePath,
        context: &str,
    ) -> Result<(), <T as NvSwitchTransport>::Failure> {
        let command = SwitchCommand::from(ShellCommand::ConfirmFile { path: path.clone() });
        let stdout = self.execute(&command).await?;

        if produced_required_output(&command, &stdout) {
            Ok(())
        } else {
            Err(<T as NvSwitchTransport>::Failure::from_message(format!(
                "{context} verification failed for {}",
                path.as_str()
            )))
        }
    }

    // ── Flows ───────────────────────────────────────────────────────

    /// Run every step of `flow` in order.
    ///
    /// Returns a [`FlowReport`] with one entry per declared step. On failure
    /// the same report comes back inside a [`FlowFailure`], so a caller can
    /// still see which steps had already taken effect on the switch.
    ///
    /// Steps are identified by their [`StepSubject`], never by command text:
    /// rendered command lines stay in this crate's tracing output. Two step
    /// policies can keep a failure from aborting the flow - an alternate
    /// command form and a [`BenignFailure`](crate::BenignFailure).
    pub async fn run_flow(
        &self,
        flow: &CommandFlow,
    ) -> Result<FlowReport, FlowFailure<<T as NvSwitchTransport>::Failure>> {
        self.run_flow_while(flow, || true).await
    }

    /// Run `flow` for as long as `should_continue` allows.
    ///
    /// `should_continue` is consulted at step boundaries only - never while a
    /// command is in flight - and a `false` answer stops the run before the
    /// next step is issued. Every step is therefore either fully attempted,
    /// with the status it earned, or never started and reported
    /// [`StepStatus::NotAttempted`]; the report never has to guess whether a
    /// command reached the switch.
    ///
    /// The returned report is marked [`FlowReport::interrupted`], which is the
    /// only thing separating a stopped run from one that ran to its natural
    /// end. Because a stop can land between a service change and the
    /// `nv config apply` / `nv config save` that persist it, the persist steps
    /// reporting `NotAttempted` is how a caller learns the change is staged on
    /// the switch but not committed.
    pub async fn run_flow_while(
        &self,
        flow: &CommandFlow,
        should_continue: impl Fn() -> bool,
    ) -> Result<FlowReport, FlowFailure<<T as NvSwitchTransport>::Failure>> {
        let host = self.transport.host();

        tracing::debug!(
            host,
            flow = flow.name,
            step_count = flow.steps.len(),
            "NVOS command flow starting"
        );

        // Seeded up front so an abort (or an early completion) leaves every
        // unreached step reported as NotAttempted rather than missing.
        let mut report = FlowReport {
            steps: flow
                .steps
                .iter()
                .map(|step| StepReport {
                    subject: step.subject.clone(),
                    status: StepStatus::NotAttempted,
                })
                .collect(),
            interrupted: false,
        };

        // Iterate over steps and step reports together, updating the step report with the outcome.
        for (index, (step, step_report)) in
            flow.steps.iter().zip(report.steps.iter_mut()).enumerate()
        {
            if !should_continue() {
                tracing::info!(
                    host,
                    flow = flow.name,
                    step_index = index,
                    "NVOS command flow stopped by caller; remaining steps not attempted"
                );

                report.interrupted = true;

                return Ok(report);
            }

            let outcome = match self.run_step(flow.name, step).await {
                Ok(outcome) => outcome,
                Err(failure) => {
                    step_report.status = StepStatus::Failed {
                        message: failure.message().to_owned(),
                    };

                    return Err(FlowFailure { failure, report });
                }
            };

            step_report.status = outcome.status;

            match outcome.flow {
                BenignOutcome::CompleteFlow => {
                    tracing::info!(
                        host,
                        flow = flow.name,
                        step_index = index,
                        "NVOS command flow completed early; remaining steps are unnecessary"
                    );

                    return Ok(report);
                }
                BenignOutcome::Continue => {}
            }

            // NVOS applies and persists configuration asynchronously behind the
            // CLI. Settling between steps keeps sequencing conservative without
            // adding delay after the flow's final command.
            if index + 1 < flow.steps.len() && !flow.settle_delay.is_zero() {
                tokio::time::sleep(flow.settle_delay).await;
            }
        }

        tracing::debug!(
            host,
            flow = flow.name,
            step_count = report.steps.len(),
            "NVOS command flow completed"
        );

        Ok(report)
    }

    /// Run one flow step, applying its fallback and benign-failure policy.
    async fn run_step(
        &self,
        flow: &'static str,
        step: &FlowStep,
    ) -> Result<StepOutcome, <T as NvSwitchTransport>::Failure> {
        let host = self.transport.host();

        let Err(error) = self.execute_checked(&step.command).await else {
            return Ok(StepOutcome::succeeded(false));
        };

        let Some(fallback) = &step.fallback else {
            return self.classify_failure(flow, step, error);
        };

        tracing::info!(
            host,
            flow,
            command = step.command.render_masked(),
            fallback_command = fallback.render_masked(),
            error = error.message(),
            "NVOS command failed; trying the alternate command form"
        );

        let Err(fallback_error) = self.execute_checked(fallback).await else {
            return Ok(StepOutcome::succeeded(true));
        };

        // A fallback exists because NVOS spells the same operation two ways.
        // Only when *both* spellings report the same benign condition is the
        // operation genuinely unnecessary; a different second failure means the
        // command form was fine and something else went wrong.
        let both_benign = step.benign.filter(|benign| {
            benign.matches(error.message()) && benign.matches(fallback_error.message())
        });

        if let Some(benign) = both_benign {
            tracing::info!(
                host,
                flow,
                command = step.command.render_masked(),
                fallback_command = fallback.render_masked(),
                "both NVOS command forms report the operation is unsupported; already in the desired state"
            );

            return Ok(StepOutcome::benign(benign.reason, benign.outcome));
        }

        // Both spellings failed with (potentially) different classifications.
        // Report both messages, but keep the PRIMARY command's classification:
        // the primary spelling is the canonical operation, and
        // `CommandFailure::with_message` is contractually classification-
        // preserving. Wrapping `fallback_error` here would silently swap in the
        // fallback's code and violate that contract.
        //
        // The step is named by its subject rather than by either command line:
        // this message reaches callers (and, through them, API clients), while
        // the command text stays in the tracing events above.
        Err(error.with_message(format!(
            "{flow} step {} failed: {}; the alternate command form also failed: {}",
            describe_step(step),
            error.message(),
            fallback_error.message(),
        )))
    }

    /// Decide whether a step failure without a fallback aborts the flow.
    fn classify_failure(
        &self,
        flow: &'static str,
        step: &FlowStep,
        error: <T as NvSwitchTransport>::Failure,
    ) -> Result<StepOutcome, <T as NvSwitchTransport>::Failure> {
        let Some(benign) = step.benign.filter(|benign| benign.matches(error.message())) else {
            return Err(error);
        };

        tracing::info!(
            host = self.transport.host(),
            flow,
            command = step.command.render_masked(),
            error = error.message(),
            "ignoring benign NVOS command failure; switch is already in the desired state"
        );

        Ok(StepOutcome::benign(benign.reason, benign.outcome))
    }

    // ── Execution ───────────────────────────────────────────────────

    /// Execute `command` and require any output the command must produce.
    async fn execute_checked(
        &self,
        command: &SwitchCommand,
    ) -> Result<String, <T as NvSwitchTransport>::Failure> {
        let stdout = self.execute(command).await?;

        if produced_required_output(command, &stdout) {
            Ok(stdout)
        } else {
            Err(<T as NvSwitchTransport>::Failure::from_message(format!(
                "`{}` did not report success on the switch",
                command.render_masked()
            )))
        }
    }

    async fn execute(
        &self,
        command: &SwitchCommand,
    ) -> Result<String, <T as NvSwitchTransport>::Failure> {
        let host = self.transport.host();
        let log_value = command.render_masked();

        tracing::debug!(
            host,
            command = &log_value,
            timeout_secs = self.config.command_timeout.as_secs(),
            "running NVOS command"
        );

        self.transport
            .run(command, self.config.command_timeout)
            .await
            .inspect_err(|error| {
                tracing::warn!(
                    host,
                    command = &log_value,
                    error = error.message(),
                    "NVOS command failed"
                );
            })
    }
}

/// A completed step's recorded status plus how the flow should proceed.
struct StepOutcome {
    status: StepStatus,
    flow: BenignOutcome,
}

impl StepOutcome {
    fn succeeded(used_fallback: bool) -> Self {
        Self {
            status: StepStatus::Succeeded { used_fallback },
            flow: BenignOutcome::Continue,
        }
    }

    fn benign(reason: BenignReason, flow: BenignOutcome) -> Self {
        Self {
            status: StepStatus::Benign { reason },
            flow,
        }
    }
}

/// Name `step` for an error message that callers may surface to a client.
///
/// Uses the step's subject rather than its command line, so a failure can be
/// attributed without the message carrying `nv` command text.
fn describe_step(step: &FlowStep) -> String {
    match step.subject() {
        Some(subject) => format!("for {}", subject.label()),
        None => "(unlabelled)".to_owned(),
    }
}

/// Whether `stdout` carries the confirmation `command` must produce.
///
/// The marker (e.g. the `ConfirmFile` `"ok"`) must appear as a line of its own,
/// not merely as a substring: a captured switch session can carry banner / MOTD
/// / prompt-bleed text alongside the command output, and an unanchored
/// `contains` would treat incidental substrings ("broken", "took", "look") as a
/// confirmation and let a job proceed on output the command never produced.
fn produced_required_output(command: &SwitchCommand, stdout: &str) -> bool {
    command
        .required_output()
        .is_none_or(|required| stdout.lines().any(|line| line.trim() == required))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::command::{ClusterApp, MtlsService, NvCommand, NvConfigOperation};
    use crate::flow::{BenignFailure, BenignReason, CommandFlow, FlowStep};

    /// Build an mTLS unset flow for a test, surfacing a construction error as a
    /// test failure rather than an unwrap.
    fn unset_mtls_flow(services: &[MtlsService]) -> CommandFlow {
        match CommandFlow::unset_mtls(services) {
            Ok(flow) => flow,
            Err(error) => panic!("test flow failed to build: {error}"),
        }
    }

    /// `(subject label, status)` for each step, the shape assertions read most
    /// naturally in.
    fn outcomes(report: &FlowReport) -> Vec<(Option<&str>, &StepStatus)> {
        report
            .steps()
            .iter()
            .map(|step| (step.subject().map(StepSubject::label), step.status()))
            .collect()
    }

    const SUCCEEDED: StepStatus = StepStatus::Succeeded {
        used_fallback: false,
    };
    const SUCCEEDED_VIA_FALLBACK: StepStatus = StepStatus::Succeeded {
        used_fallback: true,
    };
    const NOT_ATTEMPTED: StepStatus = StepStatus::NotAttempted;
    const GNMI_ABSENT: StepStatus = StepStatus::Benign {
        reason: BenignReason::GnmiMtlsSchemaAbsent,
    };
    const CLUSTER_DISABLED: StepStatus = StepStatus::Benign {
        reason: BenignReason::ClusterNotEnabled,
    };

    /// A caller-side classification the handler must never rewrite, standing in
    /// for the gRPC-style code a real `CommandFailure` carries alongside its
    /// message.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TestCode {
        Unset,
        Internal,
        Unavailable,
    }

    /// Test failure carrying the message the handler reads plus an opaque
    /// classification, mirroring how a caller's own error type is used. The
    /// classification lets tests assert `CommandFailure`'s contract that
    /// `with_message` preserves it.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestFailure {
        message: String,
        code: TestCode,
    }

    impl TestFailure {
        fn new(message: &str) -> Self {
            Self {
                message: message.to_owned(),
                code: TestCode::Unset,
            }
        }

        /// Build a failure with an explicit classification, so a test can tell
        /// two failures apart by code rather than only by message.
        fn with_code(message: &str, code: TestCode) -> Self {
            Self {
                message: message.to_owned(),
                code,
            }
        }

        fn code(&self) -> TestCode {
            self.code
        }
    }

    impl CommandFailure for TestFailure {
        fn message(&self) -> &str {
            &self.message
        }

        fn with_message(&self, message: String) -> Self {
            // Contract: keep the original classification, replace only the text.
            Self {
                message,
                code: self.code,
            }
        }

        fn from_message(message: String) -> Self {
            Self {
                message,
                code: TestCode::Unset,
            }
        }
    }

    /// Transport that records every command and replies from a scripted table.
    struct ScriptedTransport {
        recorded: Mutex<Vec<String>>,
        replies: Vec<(String, Result<String, TestFailure>)>,
        default_reply: Result<String, TestFailure>,
    }

    impl ScriptedTransport {
        fn new() -> Self {
            Self {
                recorded: Mutex::new(Vec::new()),
                replies: Vec::new(),
                default_reply: Ok(String::new()),
            }
        }

        fn reply(mut self, command: &str, reply: Result<String, TestFailure>) -> Self {
            self.replies.push((command.to_owned(), reply));
            self
        }

        fn recorded(&self) -> Vec<String> {
            self.recorded.lock().unwrap().clone()
        }
    }

    impl NvSwitchTransport for ScriptedTransport {
        type Failure = TestFailure;

        fn host(&self) -> &str {
            "10.0.0.1"
        }

        async fn run(
            &self,
            command: &SwitchCommand,
            _timeout: Duration,
        ) -> Result<String, Self::Failure> {
            let rendered = command.render();

            self.recorded.lock().unwrap().push(rendered.clone());

            self.replies
                .iter()
                .find(|(scripted, _)| *scripted == rendered)
                .map_or_else(|| self.default_reply.clone(), |(_, reply)| reply.clone())
        }
    }

    const GNMI_UNSET: &str = "nv unset system gnmi-server mtls certificate";
    const GNMI_UNSET_FALLBACK: &str = "nv unset system gnmi-server mtls ca-certificate";
    const GNMI_SCHEMA_ABSENT: &str = "Error: 'mtls' is not one of ['state', 'certificate']";
    const CONFIG_APPLY: &str = "nv config apply --assume-yes";
    const CONFIG_SAVE: &str = "nv config save";

    #[tokio::test]
    async fn run_returns_command_stdout() {
        let transport = ScriptedTransport::new().reply(CONFIG_SAVE, Ok("saved".to_owned()));
        let handler = NvSwitchCliHandler::new(transport);

        let stdout = handler
            .run(NvCommand::config(NvConfigOperation::Save))
            .await
            .unwrap();

        assert_eq!(stdout, "saved");
        assert_eq!(handler.transport().recorded(), vec![CONFIG_SAVE]);
    }

    #[tokio::test]
    async fn run_flow_executes_every_step_in_order() {
        let handler = NvSwitchCliHandler::new(ScriptedTransport::new());

        let report = handler
            .run_flow(&unset_mtls_flow(&[
                MtlsService::NvueApi,
                MtlsService::ClusterApp(ClusterApp::nmx_controller()),
            ]))
            .await
            .unwrap();

        assert_eq!(
            outcomes(&report),
            vec![
                (Some("nvue-api"), &SUCCEEDED),
                (Some("nmx-controller"), &SUCCEEDED),
                (Some("config-apply"), &SUCCEEDED),
                (Some("config-save"), &SUCCEEDED),
            ]
        );

        assert!(!report.interrupted());

        assert_eq!(
            handler.transport().recorded(),
            vec![
                "nv unset system api mtls ca-certificate",
                "nv action restore cluster apps nmx-controller manager encryption",
                CONFIG_APPLY,
                CONFIG_SAVE,
            ]
        );
    }

    /// Stopping part-way must leave the steps that ran holding the statuses
    /// they earned. Their commands have already reached the switch, so a
    /// report that called them unattempted would misstate the switch's state.
    #[tokio::test]
    async fn run_flow_while_stops_at_a_step_boundary_and_keeps_what_ran() {
        let handler = NvSwitchCliHandler::new(ScriptedTransport::new());
        let allowed = AtomicUsize::new(2);

        let report = handler
            .run_flow_while(
                &unset_mtls_flow(&[
                    MtlsService::NvueApi,
                    MtlsService::ClusterApp(ClusterApp::nmx_controller()),
                ]),
                || {
                    allowed
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                            left.checked_sub(1)
                        })
                        .is_ok()
                },
            )
            .await
            .unwrap();

        assert!(report.interrupted());
        assert_eq!(
            outcomes(&report),
            vec![
                (Some("nvue-api"), &SUCCEEDED),
                (Some("nmx-controller"), &SUCCEEDED),
                (Some("config-apply"), &NOT_ATTEMPTED),
                (Some("config-save"), &NOT_ATTEMPTED),
            ]
        );

        // Stopping means stopping: the persist commands were never issued, so
        // the two service changes are staged on the switch but not committed.
        assert_eq!(
            handler.transport().recorded(),
            vec![
                "nv unset system api mtls ca-certificate",
                "nv action restore cluster apps nmx-controller manager encryption",
            ]
        );
    }

    #[tokio::test]
    async fn run_flow_while_that_never_starts_issues_nothing() {
        let handler = NvSwitchCliHandler::new(ScriptedTransport::new());

        let report = handler
            .run_flow_while(&unset_mtls_flow(&[MtlsService::NvueApi]), || false)
            .await
            .unwrap();

        assert!(report.interrupted());
        assert!(
            report
                .steps()
                .iter()
                .all(|step| step.status() == &StepStatus::NotAttempted)
        );
        assert!(handler.transport().recorded().is_empty());
    }

    /// An abort must still yield a report: the steps before it took effect on
    /// the switch, and the caller needs to know which ones.
    #[tokio::test]
    async fn run_flow_stops_at_the_first_fatal_failure() {
        let transport = ScriptedTransport::new().reply(
            "nv action restore cluster apps nmx-controller manager encryption",
            Err(TestFailure::new("permission denied")),
        );
        let handler = NvSwitchCliHandler::new(transport);

        let error = handler
            .run_flow(&unset_mtls_flow(&[
                MtlsService::NvueApi,
                MtlsService::ClusterApp(ClusterApp::nmx_controller()),
            ]))
            .await
            .unwrap_err();

        assert_eq!(error.failure, TestFailure::new("permission denied"));

        let failed = StepStatus::Failed {
            message: "permission denied".to_owned(),
        };

        assert_eq!(
            outcomes(&error.report),
            vec![
                (Some("nvue-api"), &SUCCEEDED),
                (Some("nmx-controller"), &failed),
                (Some("config-apply"), &NOT_ATTEMPTED),
                (Some("config-save"), &NOT_ATTEMPTED),
            ]
        );

        assert_eq!(
            error.report.failed_step().and_then(StepReport::subject),
            Some(&StepSubject::Mtls(MtlsService::ClusterApp(
                ClusterApp::nmx_controller()
            )))
        );

        assert_eq!(
            handler.transport().recorded(),
            vec![
                "nv unset system api mtls ca-certificate",
                "nv action restore cluster apps nmx-controller manager encryption",
            ]
        );
    }

    #[tokio::test]
    async fn run_flow_continues_past_a_benign_failure() {
        let transport = ScriptedTransport::new().reply(
            "nv action restore cluster apps nmx-controller manager encryption",
            Err(TestFailure::new(
                "Action failed with the following issue: cluster is not enabled",
            )),
        );
        let handler = NvSwitchCliHandler::new(transport);

        let report = handler
            .run_flow(&unset_mtls_flow(&[MtlsService::ClusterApp(
                ClusterApp::nmx_controller(),
            )]))
            .await
            .unwrap();

        assert_eq!(
            outcomes(&report),
            vec![
                (Some("nmx-controller"), &CLUSTER_DISABLED),
                (Some("config-apply"), &SUCCEEDED),
                (Some("config-save"), &SUCCEEDED),
            ]
        );
    }

    #[tokio::test]
    async fn already_applied_mtls_clear_still_saves_configuration() {
        let transport = ScriptedTransport::new().reply(
            CONFIG_APPLY,
            Err(TestFailure::new(
                "Revision 'applied' has already been applied.",
            )),
        );
        let handler = NvSwitchCliHandler::new(transport);

        let report = handler
            .run_flow(&unset_mtls_flow(&[MtlsService::NvueApi]))
            .await
            .unwrap();

        assert_eq!(
            outcomes(&report),
            vec![
                (Some("nvue-api"), &SUCCEEDED),
                (
                    Some("config-apply"),
                    &StepStatus::Benign {
                        reason: BenignReason::ConfigAlreadyApplied,
                    }
                ),
                (Some("config-save"), &SUCCEEDED),
            ]
        );
        assert_eq!(
            handler.transport().recorded(),
            vec![
                "nv unset system api mtls ca-certificate",
                CONFIG_APPLY,
                CONFIG_SAVE
            ]
        );
    }

    #[tokio::test]
    async fn already_applied_tolerance_is_limited_to_apply_of_the_applied_revision() {
        for (command, message, expected_stage) in [
            (
                "nv unset system api mtls ca-certificate",
                "Revision 'applied' has already been applied.",
                "nvue-api",
            ),
            (
                CONFIG_SAVE,
                "Revision 'applied' has already been applied.",
                "config-save",
            ),
            (
                CONFIG_APPLY,
                "Revision 'pending' has already been applied.",
                "config-apply",
            ),
            (CONFIG_APPLY, "permission denied", "config-apply"),
        ] {
            let handler = NvSwitchCliHandler::new(
                ScriptedTransport::new().reply(command, Err(TestFailure::new(message))),
            );
            let failure = handler
                .run_flow(&unset_mtls_flow(&[MtlsService::NvueApi]))
                .await
                .unwrap_err();
            assert_eq!(
                failure
                    .report
                    .failed_step()
                    .and_then(StepReport::subject)
                    .map(StepSubject::label),
                Some(expected_stage),
            );
            assert_eq!(
                handler.transport().recorded().last().map(String::as_str),
                Some(command)
            );
        }
    }

    #[tokio::test]
    async fn run_flow_falls_back_to_the_alternate_command_form() {
        let transport = ScriptedTransport::new().reply(
            GNMI_UNSET,
            Err(TestFailure::new("unknown field certificate")),
        );
        let handler = NvSwitchCliHandler::new(transport);

        let report = handler
            .run_flow(&unset_mtls_flow(&[MtlsService::GnmiServer]))
            .await
            .unwrap();

        assert_eq!(
            outcomes(&report),
            vec![
                (Some("gnmi-server"), &SUCCEEDED_VIA_FALLBACK),
                (Some("config-apply"), &SUCCEEDED),
                (Some("config-save"), &SUCCEEDED),
            ]
        );

        assert_eq!(
            handler.transport().recorded(),
            vec![GNMI_UNSET, GNMI_UNSET_FALLBACK, CONFIG_APPLY, CONFIG_SAVE]
        );
    }

    #[tokio::test]
    async fn run_flow_completes_early_when_both_forms_report_an_absent_schema() {
        let transport = ScriptedTransport::new()
            .reply(GNMI_UNSET, Err(TestFailure::new(GNMI_SCHEMA_ABSENT)))
            .reply(
                GNMI_UNSET_FALLBACK,
                Err(TestFailure::new(GNMI_SCHEMA_ABSENT)),
            );
        let handler = NvSwitchCliHandler::new(transport);

        let report = handler
            .run_flow(&unset_mtls_flow(&[MtlsService::GnmiServer]))
            .await
            .unwrap();

        // Completing early must not drop the persist steps from the report:
        // "never ran" and "unknown" are different answers for the caller.
        assert_eq!(
            outcomes(&report),
            vec![
                (Some("gnmi-server"), &GNMI_ABSENT),
                (Some("config-apply"), &NOT_ATTEMPTED),
                (Some("config-save"), &NOT_ATTEMPTED),
            ]
        );

        assert_eq!(
            handler.transport().recorded(),
            vec![GNMI_UNSET, GNMI_UNSET_FALLBACK]
        );
    }

    #[tokio::test]
    async fn absent_gnmi_schema_still_applies_config_for_other_services() {
        let transport = ScriptedTransport::new()
            .reply(GNMI_UNSET, Err(TestFailure::new(GNMI_SCHEMA_ABSENT)))
            .reply(
                GNMI_UNSET_FALLBACK,
                Err(TestFailure::new(GNMI_SCHEMA_ABSENT)),
            );
        let handler = NvSwitchCliHandler::new(transport);

        let report = handler
            .run_flow(&unset_mtls_flow(&[
                MtlsService::NvueApi,
                MtlsService::GnmiServer,
            ]))
            .await
            .unwrap();

        assert_eq!(
            outcomes(&report),
            vec![
                (Some("nvue-api"), &SUCCEEDED),
                (Some("gnmi-server"), &GNMI_ABSENT),
                (Some("config-apply"), &SUCCEEDED),
                (Some("config-save"), &SUCCEEDED),
            ]
        );

        assert_eq!(
            handler.transport().recorded(),
            vec![
                "nv unset system api mtls ca-certificate",
                GNMI_UNSET,
                GNMI_UNSET_FALLBACK,
                CONFIG_APPLY,
                CONFIG_SAVE,
            ]
        );
    }

    /// Both spellings failing must report both switch messages and name the
    /// step by its subject -- never by either command line, since this text
    /// reaches callers while the commands stay in tracing.
    #[tokio::test]
    async fn a_non_benign_fallback_failure_reports_both_errors_without_command_text() {
        let transport = ScriptedTransport::new()
            .reply(GNMI_UNSET, Err(TestFailure::new(GNMI_SCHEMA_ABSENT)))
            .reply(
                GNMI_UNSET_FALLBACK,
                Err(TestFailure::new("permission denied")),
            );
        let handler = NvSwitchCliHandler::new(transport);

        let error = handler
            .run_flow(&unset_mtls_flow(&[MtlsService::GnmiServer]))
            .await
            .unwrap_err();

        for fragment in ["gnmi-server", "'mtls' is not one of", "permission denied"] {
            assert!(
                error.failure.message().contains(fragment),
                "expected {fragment:?} in {:?}",
                error.failure.message()
            );
        }

        for command in [GNMI_UNSET, GNMI_UNSET_FALLBACK, "nv unset"] {
            assert!(
                !error.failure.message().contains(command),
                "command text {command:?} leaked into {:?}",
                error.failure.message()
            );
        }

        assert_eq!(
            handler.transport().recorded(),
            vec![GNMI_UNSET, GNMI_UNSET_FALLBACK]
        );
    }

    #[tokio::test]
    async fn a_benign_primary_failure_does_not_excuse_a_different_fallback_failure() {
        let transport = ScriptedTransport::new()
            .reply(GNMI_UNSET, Err(TestFailure::new(GNMI_SCHEMA_ABSENT)))
            .reply(GNMI_UNSET_FALLBACK, Err(TestFailure::new("device busy")));
        let handler = NvSwitchCliHandler::new(transport);

        let error = handler
            .run_flow(&unset_mtls_flow(&[MtlsService::GnmiServer]))
            .await
            .unwrap_err();

        assert!(error.failure.message().contains("device busy"));
    }

    #[tokio::test]
    async fn a_double_failure_keeps_the_primary_commands_classification() {
        // Primary and fallback both fail non-benignly, with DIFFERENT
        // classifications. The combined error must report both messages but
        // carry the PRIMARY command's code: the primary spelling is the
        // canonical operation, and with_message() is classification-preserving.
        let transport = ScriptedTransport::new()
            .reply(
                GNMI_UNSET,
                Err(TestFailure::with_code("primary boom", TestCode::Internal)),
            )
            .reply(
                GNMI_UNSET_FALLBACK,
                Err(TestFailure::with_code(
                    "fallback boom",
                    TestCode::Unavailable,
                )),
            );
        let handler = NvSwitchCliHandler::new(transport);

        let error = handler
            .run_flow(&unset_mtls_flow(&[MtlsService::GnmiServer]))
            .await
            .unwrap_err();

        assert_eq!(
            error.failure.code(),
            TestCode::Internal,
            "the primary command's classification must win, not the fallback's ({:?})",
            error.failure.code()
        );
        // The message still reports both failures for diagnosis.
        assert!(error.failure.message().contains("primary boom"));
        assert!(error.failure.message().contains("fallback boom"));
    }

    #[tokio::test]
    async fn confirm_file_accepts_the_expected_marker() {
        let path = RemotePath::new("/host/nos-images/nvos.bin").unwrap();
        let transport = ScriptedTransport::new().reply(
            "test -f '/host/nos-images/nvos.bin' && echo ok",
            Ok("ok\n".to_owned()),
        );
        let handler = NvSwitchCliHandler::new(transport);

        handler.confirm_file(&path, "SFTP upload").await.unwrap();
    }

    #[tokio::test]
    async fn confirm_file_reports_the_caller_context_when_the_marker_is_missing() {
        let path = RemotePath::new("/host/nos-images/nvos.bin").unwrap();
        let transport = ScriptedTransport::new().reply(
            "test -f '/host/nos-images/nvos.bin' && echo ok",
            Ok(String::new()),
        );
        let handler = NvSwitchCliHandler::new(transport);

        let error = handler
            .confirm_file(&path, "SFTP upload")
            .await
            .unwrap_err();

        assert_eq!(
            error.message(),
            "SFTP upload verification failed for /host/nos-images/nvos.bin"
        );
    }

    #[tokio::test]
    async fn confirm_file_propagates_the_transport_failure_unchanged() {
        let path = RemotePath::new("/host/nos-images/nvos.bin").unwrap();
        let transport = ScriptedTransport::new().reply(
            "test -f '/host/nos-images/nvos.bin' && echo ok",
            Err(TestFailure::new("SSH command failed with exit status 1")),
        );
        let handler = NvSwitchCliHandler::new(transport);

        let error = handler
            .confirm_file(&path, "SFTP upload")
            .await
            .unwrap_err();

        assert_eq!(error.message(), "SSH command failed with exit status 1");
    }

    #[tokio::test]
    async fn shell_helpers_render_their_commands() {
        let path = RemotePath::new("/home/admin/certs/ca.pem").unwrap();
        let handler = NvSwitchCliHandler::new(ScriptedTransport::new());

        handler
            .create_directory(
                &path,
                FileMode::builder()
                    .owner_read()
                    .owner_write()
                    .owner_execute()
                    .group_read()
                    .group_execute()
                    .other_execute()
                    .build(),
            )
            .await
            .unwrap();
        handler.create_directory_tree(&path).await.unwrap();
        handler
            .set_permissions(&path, FileMode::builder().owner_read().build())
            .await
            .unwrap();
        handler.remove_file(&path).await.unwrap();
        handler.remove_directory(&path).await.unwrap();

        assert_eq!(
            handler.transport().recorded(),
            vec![
                "install -d -m 751 '/home/admin/certs/ca.pem'",
                "mkdir -p '/home/admin/certs/ca.pem'",
                "chmod 400 '/home/admin/certs/ca.pem'",
                "rm -f '/home/admin/certs/ca.pem'",
                "rm -rf '/home/admin/certs/ca.pem'",
            ]
        );
    }

    /// Commands are run exactly once; a failure is never re-issued.
    #[tokio::test]
    async fn a_failing_command_is_run_exactly_once() {
        let transport =
            ScriptedTransport::new().reply(CONFIG_SAVE, Err(TestFailure::new("connection reset")));
        let handler = NvSwitchCliHandler::new(transport);

        handler
            .run(NvCommand::config(NvConfigOperation::Save))
            .await
            .unwrap_err();

        assert_eq!(handler.transport().recorded(), vec![CONFIG_SAVE]);
    }

    #[tokio::test(start_paused = true)]
    async fn run_flow_settles_between_steps_but_not_after_the_last() {
        let settle = Duration::from_secs(5);
        let flow = CommandFlow::new("settle")
            .with_settle_delay(settle)
            .push(NvCommand::config(NvConfigOperation::ApplyAssumeYes))
            .push(NvCommand::config(NvConfigOperation::Save));

        let handler = NvSwitchCliHandler::new(ScriptedTransport::new());
        let started = tokio::time::Instant::now();

        handler.run_flow(&flow).await.unwrap();

        assert_eq!(started.elapsed(), settle);
    }

    #[tokio::test]
    async fn a_flow_step_requiring_output_fails_when_the_marker_is_absent() {
        let path = RemotePath::new("/tmp/staged.bin").unwrap();
        let flow = CommandFlow::new("verify").push(ShellCommand::ConfirmFile { path });

        let handler = NvSwitchCliHandler::new(ScriptedTransport::new());

        let error = handler.run_flow(&flow).await.unwrap_err();

        assert!(error.failure.message().contains("did not report success"));
    }

    #[tokio::test]
    async fn a_step_whose_benign_policy_does_not_match_still_fails() {
        let step = FlowStep::new(NvCommand::config(NvConfigOperation::Save)).with_benign_failure(
            BenignFailure::new(BenignReason::ClusterNotEnabled, BenignOutcome::Continue),
        );

        let transport =
            ScriptedTransport::new().reply(CONFIG_SAVE, Err(TestFailure::new("disk full")));
        let handler = NvSwitchCliHandler::new(transport);

        let error = handler
            .run_flow(&CommandFlow::new("save").push(step))
            .await
            .unwrap_err();

        assert_eq!(error.failure.message(), "disk full");
        assert_eq!(
            error.report.steps()[0].status(),
            &StepStatus::Failed {
                message: "disk full".to_owned()
            }
        );
    }

    /// An unlabelled step still reports, and its failure message says so
    /// rather than falling back to quoting the command.
    #[tokio::test]
    async fn an_unlabelled_step_reports_without_a_subject() {
        let transport =
            ScriptedTransport::new().reply(CONFIG_SAVE, Err(TestFailure::new("disk full")));
        let handler = NvSwitchCliHandler::new(transport);
        let flow = CommandFlow::new("save").push(NvCommand::config(NvConfigOperation::Save));

        let error = handler.run_flow(&flow).await.unwrap_err();

        assert_eq!(outcomes(&error.report).len(), 1);
        assert_eq!(error.report.steps()[0].subject(), None);
    }

    #[tokio::test]
    async fn an_empty_flow_runs_nothing() {
        let handler = NvSwitchCliHandler::new(ScriptedTransport::new());

        let report = handler.run_flow(&CommandFlow::new("empty")).await.unwrap();

        assert!(report.is_empty());
        assert!(report.failed_step().is_none());
        assert!(handler.transport().recorded().is_empty());
    }

    /// The confirmation marker must appear as a line of its own. An unanchored
    /// substring check would treat banner / prompt text that merely contains
    /// "ok" ("broken", "took", "look") as a spurious confirmation.
    #[test]
    fn required_output_matches_the_marker_only_on_its_own_line() {
        let path = RemotePath::new("/tmp/staged.bin").unwrap();
        let confirm = SwitchCommand::from(ShellCommand::ConfirmFile { path });

        for stdout in ["ok", "ok\n", "banner line\nok\n", "  ok  \n"] {
            assert!(
                produced_required_output(&confirm, stdout),
                "expected {stdout:?} to confirm"
            );
        }

        for stdout in [
            "Connection to switch broken, retry later",
            "that took a while",
            "please look again",
            "okay",
            "",
        ] {
            assert!(
                !produced_required_output(&confirm, stdout),
                "unexpected confirmation for {stdout:?}"
            );
        }
    }

    #[test]
    fn commands_without_required_output_always_pass() {
        let save = SwitchCommand::from(NvCommand::config(NvConfigOperation::Save));

        assert!(produced_required_output(&save, "anything at all"));
    }
}
