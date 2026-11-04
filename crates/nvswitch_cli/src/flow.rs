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

//! Ordered command sequences and the policy applied when a step fails.
use std::time::Duration;

use crate::command::{MtlsService, NvCommand, NvConfigOperation, SwitchCommand};
use crate::error::Result;

/// What a flow does after a step fails in a way classified as benign.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BenignOutcome {
    /// Move on to the next step.
    Continue,

    /// Stop the flow and report success.
    CompleteFlow,
}

/// Why a step failure is classified as benign.
///
/// Naming the condition - rather than only the message fragments that detect
/// it - lets a run report say *why* a step was tolerated, so a caller can
/// distinguish "the switch was already in the desired state" from "this NVOS
/// build has no such setting" without re-matching error text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum BenignReason {
    /// NVOS rejects cluster app actions when the cluster is disabled. The
    /// desired end state - no cluster app mTLS - already holds.
    ClusterNotEnabled,

    /// This NVOS build has no gNMI mTLS schema, so the service is already
    /// insecure and there is nothing to clear.
    GnmiMtlsSchemaAbsent,

    /// NVOS has no pending revision after the bindings were cleared; the
    /// selected `applied` revision already holds the requested configuration.
    ConfigAlreadyApplied,
}

impl BenignReason {
    /// Message fragments that identify this condition in NVOS error text.
    ///
    /// A build without a gNMI mTLS schema rejects both spellings of the leaf,
    /// quoting the field name back with either quote style.
    pub fn message_fragments(self) -> &'static [&'static str] {
        match self {
            Self::ClusterNotEnabled => &["cluster is not enabled"],
            Self::GnmiMtlsSchemaAbsent => &["'mtls' is not one of", "\"mtls\" is not one of"],
            Self::ConfigAlreadyApplied => &["Revision 'applied' has already been applied"],
        }
    }
}

/// A step failure that does not abort the flow.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BenignFailure {
    /// The condition that makes the failure benign.
    pub reason: BenignReason,

    /// What the flow does after a benign failure.
    pub outcome: BenignOutcome,
}

impl BenignFailure {
    /// Tolerate `reason`, then proceed according to `outcome`.
    pub fn new(reason: BenignReason, outcome: BenignOutcome) -> Self {
        Self { reason, outcome }
    }

    /// Whether `message` matches any fragment of this failure's reason.
    ///
    /// Matching is case-insensitive: NVOS has varied the casing of these error
    /// strings across builds, and the same NVOS messages are already classified
    /// case-insensitively elsewhere in RMS (`contains_case_insensitive` in
    /// `switch_gb200_nvidia::system_image`). A case-sensitive match here would
    /// hard-fail a step that should have been treated as benign.
    pub fn matches(&self, message: &str) -> bool {
        let message = message.to_ascii_lowercase();
        self.reason
            .message_fragments()
            .iter()
            .any(|fragment| message.contains(&fragment.to_ascii_lowercase()))
    }
}

/// What a [`FlowStep`] acts on, independent of the command text that does it.
///
/// A step's rendered command line is an implementation detail of *how* NVOS is
/// driven; the subject is *what* was driven. Reports built from subjects can
/// therefore attribute an outcome to a service or a persist operation without
/// carrying (and potentially leaking) command strings.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum StepSubject {
    /// The step clears a service's mTLS binding.
    Mtls(MtlsService),

    /// The step persists the running configuration.
    Persist(NvConfigOperation),
}

impl StepSubject {
    /// A stable, lowercase identifier for this subject, suitable for logs and
    /// structured reports.
    pub fn label(&self) -> &str {
        match self {
            Self::Mtls(service) => service.label(),
            Self::Persist(operation) => match operation {
                NvConfigOperation::Apply | NvConfigOperation::ApplyAssumeYes => "config-apply",
                NvConfigOperation::Save => "config-save",
                NvConfigOperation::Detach => "config-detach",
            },
        }
    }
}

/// One command in a [`CommandFlow`], with its failure policy.
#[derive(Clone, PartialEq, Eq)]
pub struct FlowStep {
    /// The command this step runs.
    pub(crate) command: SwitchCommand,

    /// Alternate command attempted when `command` fails.
    pub(crate) fallback: Option<SwitchCommand>,

    /// Failures that do not abort the flow.
    ///
    /// When `fallback` is set, this is consulted only if *both* commands fail.
    pub(crate) benign: Option<BenignFailure>,

    /// What this step acts on, reported back to callers in place of the
    /// step's command text.
    pub(crate) subject: Option<StepSubject>,
}

impl FlowStep {
    /// Build a step that aborts the flow if its command fails.
    pub fn new(command: impl Into<SwitchCommand>) -> Self {
        Self {
            command: command.into(),
            fallback: None,
            benign: None,
            subject: None,
        }
    }

    /// Attempt `fallback` when this step's command fails.
    ///
    /// The step succeeds if either command succeeds. Both commands are
    /// reported as executed when the fallback runs.
    pub fn with_fallback(mut self, fallback: impl Into<SwitchCommand>) -> Self {
        self.fallback = Some(fallback.into());
        self
    }

    /// Treat failures matching `benign` as non-fatal.
    ///
    /// With a fallback configured, the classification is applied only when both
    /// the command and its fallback fail, and only when *both* failures match.
    pub fn with_benign_failure(mut self, benign: BenignFailure) -> Self {
        self.benign = Some(benign);
        self
    }

    /// Record what this step acts on, so a run report can attribute the
    /// step's outcome without quoting its command.
    pub fn with_subject(mut self, subject: StepSubject) -> Self {
        self.subject = Some(subject);
        self
    }

    /// What this step acts on, or `None` for an unlabelled step.
    pub fn subject(&self) -> Option<&StepSubject> {
        self.subject.as_ref()
    }
}

impl<T: Into<SwitchCommand>> From<T> for FlowStep {
    fn from(command: T) -> Self {
        Self::new(command)
    }
}

/// An ordered sequence of commands executed as one unit.
///
/// # Examples
///
/// ```
/// use nvswitch_cli::{CommandFlow, MtlsService, ClusterApp};
///
/// let flow = CommandFlow::unset_mtls(&[
///     MtlsService::NvueApi,
///     MtlsService::ClusterApp(ClusterApp::nmx_controller()),
/// ])?;
///
/// assert_eq!(
///     flow.commands(),
///     vec![
///         "nv unset system api mtls ca-certificate".to_owned(),
///         "nv action restore cluster apps nmx-controller manager encryption".to_owned(),
///         "nv config apply --assume-yes".to_owned(),
///         "nv config save".to_owned(),
///     ],
/// );
/// # Ok::<(), nvswitch_cli::InvalidCommand>(())
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct CommandFlow {
    /// Flow name, used as a log field.
    pub(crate) name: &'static str,

    /// Steps in execution order.
    pub(crate) steps: Vec<FlowStep>,

    /// Delay inserted after each step except the last.
    pub(crate) settle_delay: Duration,
}

impl CommandFlow {
    /// Start an empty flow named `name`.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            steps: Vec::new(),
            settle_delay: Duration::ZERO,
        }
    }

    /// Wait `delay` after each step except the last.
    pub fn with_settle_delay(mut self, delay: Duration) -> Self {
        self.settle_delay = delay;
        self
    }

    /// Append a step.
    pub fn push(mut self, step: impl Into<FlowStep>) -> Self {
        self.steps.push(step.into());
        self
    }

    /// The flow's name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Whether the flow has no steps.
    pub fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    /// The rendered command line of each step, excluding fallbacks.
    ///
    /// Fallbacks are omitted because they only run when their primary fails, so
    /// this is what the flow executes on a fully successful run.
    pub fn commands(&self) -> Vec<String> {
        self.steps
            .iter()
            .map(|step| step.command.render())
            .collect()
    }

    // ── Named flows ─────────────────────────────────────────────────

    /// Clear each service's mTLS binding, then apply and save the result.
    ///
    /// The ordering is load-bearing in two ways. `nv config apply` and
    /// `nv config save` trail every unset, so a partially cleared binding is
    /// never persisted; and `save` follows `apply`, so what is persisted is
    /// what was applied.
    ///
    /// Two NVOS quirks are encoded as step policy:
    ///
    /// - The gNMI mTLS leaf is spelled `certificate` on some builds and
    ///   `ca-certificate` on others, so that step carries a fallback.
    /// - Builds with no gNMI mTLS schema at all reject both spellings. That is
    ///   benign - the service is already insecure - so the flow continues. If
    ///   gNMI was the *only* requested service there is nothing left to apply,
    ///   and the flow completes without touching the running configuration.
    ///
    /// Restoring a cluster app's manager encryption is likewise benign when the
    /// cluster is disabled: the app cannot be running mTLS.
    /// If applying the cleared bindings reports that the `applied` revision
    /// was already applied, continue to save that configuration.
    ///
    /// An empty `services` slice produces an empty flow. Falling through to the
    /// persist steps would apply and save the switch's running configuration on
    /// behalf of a request that asked for no change at all.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidCommand`](crate::InvalidCommand) if one of the step
    /// commands cannot be built. Every path segment involved is either a
    /// literal or already validated by its own type, so this cannot fail in
    /// practice.
    pub fn unset_mtls(services: &[MtlsService]) -> Result<Self> {
        if services.is_empty() {
            return Ok(Self::new("unset_mtls"));
        }

        // With gNMI as the only requested service, an absent schema leaves
        // nothing to apply or save.
        let gnmi_absent_outcome = if services
            .iter()
            .all(|service| matches!(service, MtlsService::GnmiServer))
        {
            BenignOutcome::CompleteFlow
        } else {
            BenignOutcome::Continue
        };

        let unset_steps = services
            .iter()
            .map(|service| {
                let step = FlowStep::new(NvCommand::unset_mtls(service)?)
                    .with_subject(StepSubject::Mtls(service.clone()));

                let step = match service {
                    MtlsService::GnmiServer => step
                        .with_fallback(NvCommand::unset_gnmi_server_mtls_ca_certificate()?)
                        .with_benign_failure(BenignFailure::new(
                            BenignReason::GnmiMtlsSchemaAbsent,
                            gnmi_absent_outcome,
                        )),
                    MtlsService::ClusterApp(_) => step.with_benign_failure(BenignFailure::new(
                        BenignReason::ClusterNotEnabled,
                        BenignOutcome::Continue,
                    )),
                    MtlsService::NvueApi => step,
                };

                Ok(step)
            })
            .collect::<Result<Vec<_>>>()?;

        let persist_steps =
            [NvConfigOperation::ApplyAssumeYes, NvConfigOperation::Save].map(|operation| {
                let step = FlowStep::new(NvCommand::config(operation))
                    .with_subject(StepSubject::Persist(operation));
                if operation == NvConfigOperation::ApplyAssumeYes {
                    step.with_benign_failure(BenignFailure::new(
                        BenignReason::ConfigAlreadyApplied,
                        BenignOutcome::Continue,
                    ))
                } else {
                    step
                }
            });

        Ok(unset_steps
            .into_iter()
            .chain(persist_steps)
            .fold(Self::new("unset_mtls"), Self::push))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::{ClusterApp, FileMode, RemotePath, ShellCommand};

    /// The benign-failure policy on `flow`'s step at `index`.
    fn step_benign(flow: &CommandFlow, index: usize) -> BenignFailure {
        match flow.steps[index].benign {
            Some(benign) => benign,
            None => panic!("step {index} has no benign-failure policy"),
        }
    }

    #[test]
    fn unset_mtls_orders_service_unsets_before_apply_and_save() -> Result<()> {
        let flow = CommandFlow::unset_mtls(&[
            MtlsService::NvueApi,
            MtlsService::ClusterApp(ClusterApp::nmx_controller()),
            MtlsService::GnmiServer,
        ])?;

        assert_eq!(
            flow.commands(),
            vec![
                "nv unset system api mtls ca-certificate",
                "nv action restore cluster apps nmx-controller manager encryption",
                "nv unset system gnmi-server mtls certificate",
                "nv config apply --assume-yes",
                "nv config save",
            ]
        );

        Ok(())
    }

    #[test]
    fn unset_mtls_attaches_a_fallback_only_to_the_gnmi_step() -> Result<()> {
        let flow = CommandFlow::unset_mtls(&[MtlsService::NvueApi, MtlsService::GnmiServer])?;

        let fallbacks: Vec<Option<String>> = flow
            .steps
            .iter()
            .map(|step| step.fallback.as_ref().map(SwitchCommand::render))
            .collect();

        assert_eq!(
            fallbacks,
            vec![
                None,
                Some("nv unset system gnmi-server mtls ca-certificate".to_owned()),
                None,
                None,
            ]
        );

        Ok(())
    }

    #[test]
    fn gnmi_only_flow_completes_instead_of_applying_when_the_schema_is_absent() -> Result<()> {
        let flow = CommandFlow::unset_mtls(&[MtlsService::GnmiServer])?;

        assert_eq!(step_benign(&flow, 0).outcome, BenignOutcome::CompleteFlow);

        Ok(())
    }

    #[test]
    fn gnmi_alongside_other_services_continues_when_the_schema_is_absent() -> Result<()> {
        let flow = CommandFlow::unset_mtls(&[MtlsService::NvueApi, MtlsService::GnmiServer])?;

        assert_eq!(step_benign(&flow, 1).outcome, BenignOutcome::Continue);

        Ok(())
    }

    #[test]
    fn cluster_app_unset_tolerates_a_disabled_cluster() -> Result<()> {
        let flow =
            CommandFlow::unset_mtls(&[MtlsService::ClusterApp(ClusterApp::nmx_telemetry())])?;
        let benign = step_benign(&flow, 0);

        assert!(benign.matches("Action failed with the following issue: cluster is not enabled"));
        assert!(!benign.matches("permission denied"));
        assert_eq!(benign.outcome, BenignOutcome::Continue);

        Ok(())
    }

    /// No requested services means no change was asked for, so the flow must
    /// not reach `nv config apply` / `nv config save` and write the switch's
    /// running configuration anyway.
    #[test]
    fn unset_mtls_with_no_services_persists_nothing() -> Result<()> {
        let flow = CommandFlow::unset_mtls(&[])?;

        assert!(flow.is_empty());
        assert!(flow.commands().is_empty());

        Ok(())
    }

    #[test]
    fn nvue_api_unset_has_no_failure_tolerance() -> Result<()> {
        let flow = CommandFlow::unset_mtls(&[MtlsService::NvueApi])?;

        assert_eq!(flow.steps[0].benign, None);
        assert_eq!(flow.steps[0].fallback, None);

        Ok(())
    }

    #[test]
    fn benign_failure_matches_either_quote_style_for_absent_gnmi_schema() {
        let benign =
            BenignFailure::new(BenignReason::GnmiMtlsSchemaAbsent, BenignOutcome::Continue);

        assert!(benign.matches("Error: 'mtls' is not one of ['state', 'certificate']"));
        assert!(benign.matches("Error: \"mtls\" is not one of [\"state\"]"));
        assert!(!benign.matches("unknown field certificate"));
    }

    /// NVOS has varied the casing of these error strings across builds, so the
    /// match must be case-insensitive (mirroring `contains_case_insensitive` in
    /// `switch_gb200_nvidia::system_image`).
    #[test]
    fn benign_failure_matching_is_case_insensitive() {
        let cluster = BenignFailure::new(BenignReason::ClusterNotEnabled, BenignOutcome::Continue);

        assert!(cluster.matches("Action failed: Cluster Is Not Enabled"));
        assert!(cluster.matches("CLUSTER IS NOT ENABLED"));
        assert!(cluster.matches("cluster is not enabled"));
        assert!(!cluster.matches("cluster is enabled"));

        let gnmi = BenignFailure::new(BenignReason::GnmiMtlsSchemaAbsent, BenignOutcome::Continue);

        assert!(gnmi.matches("Error: 'MTLS' is NOT one of ['state']"));
    }

    /// Each reason detects only its own NVOS condition; a shared fragment
    /// table would let a disabled cluster excuse an absent gNMI schema.
    #[test]
    fn benign_reasons_do_not_match_each_others_conditions() {
        let cluster = BenignFailure::new(BenignReason::ClusterNotEnabled, BenignOutcome::Continue);
        let gnmi = BenignFailure::new(BenignReason::GnmiMtlsSchemaAbsent, BenignOutcome::Continue);

        assert!(!cluster.matches("Error: 'mtls' is not one of ['state']"));
        assert!(!gnmi.matches("Action failed: cluster is not enabled"));
    }

    /// Every step the unset flow builds must be attributable without reading
    /// its command text, since that text never reaches a caller's report.
    #[test]
    fn unset_mtls_tags_every_step_with_its_subject() -> Result<()> {
        let flow = CommandFlow::unset_mtls(&[
            MtlsService::NvueApi,
            MtlsService::ClusterApp(ClusterApp::nmx_telemetry()),
            MtlsService::GnmiServer,
        ])?;

        let subjects: Vec<Option<&StepSubject>> =
            flow.steps.iter().map(FlowStep::subject).collect();

        assert_eq!(
            subjects,
            vec![
                Some(&StepSubject::Mtls(MtlsService::NvueApi)),
                Some(&StepSubject::Mtls(MtlsService::ClusterApp(
                    ClusterApp::nmx_telemetry()
                ))),
                Some(&StepSubject::Mtls(MtlsService::GnmiServer)),
                Some(&StepSubject::Persist(NvConfigOperation::ApplyAssumeYes)),
                Some(&StepSubject::Persist(NvConfigOperation::Save)),
            ]
        );

        Ok(())
    }

    #[test]
    fn step_subject_labels_are_stable_identifiers() {
        let cases = [
            (StepSubject::Mtls(MtlsService::NvueApi), "nvue-api"),
            (StepSubject::Mtls(MtlsService::GnmiServer), "gnmi-server"),
            (
                StepSubject::Mtls(MtlsService::ClusterApp(ClusterApp::nmx_controller())),
                "nmx-controller",
            ),
            (
                StepSubject::Persist(NvConfigOperation::ApplyAssumeYes),
                "config-apply",
            ),
            (StepSubject::Persist(NvConfigOperation::Save), "config-save"),
        ];

        for (subject, expected) in cases {
            assert_eq!(subject.label(), expected);
        }
    }

    /// A step built without a subject stays unlabelled rather than silently
    /// inheriting one.
    #[test]
    fn a_plain_step_has_no_subject() {
        let step = FlowStep::new(NvCommand::config(NvConfigOperation::Save));

        assert_eq!(step.subject(), None);
    }

    #[test]
    fn flows_can_mix_nv_and_shell_commands() {
        let path = RemotePath::new("/home/admin/certs").unwrap();

        let flow = CommandFlow::new("stage_certs")
            .push(ShellCommand::CreateDirectory {
                path: path.clone(),
                mode: FileMode::builder()
                    .owner_read()
                    .owner_write()
                    .owner_execute()
                    .group_read()
                    .group_execute()
                    .other_execute()
                    .build(),
            })
            .push(ShellCommand::ConfirmFile { path })
            .push(NvCommand::config(NvConfigOperation::Save));

        assert_eq!(
            flow.commands(),
            vec![
                "install -d -m 751 '/home/admin/certs'",
                "test -f '/home/admin/certs' && echo ok",
                "nv config save",
            ]
        );
    }

    #[test]
    fn new_flow_is_empty_and_named() {
        let flow = CommandFlow::new("noop");

        assert!(flow.is_empty());
        assert_eq!(flow.name(), "noop");
        assert_eq!(flow.settle_delay, Duration::ZERO);
    }
}
