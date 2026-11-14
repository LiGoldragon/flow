//! The translator of Flow's Emission (`ethos/emission.ethos`): the one place
//! that holds the words each emission is spoken in, and that makes each
//! external call and observes its outcome.

use crate::generated::emission::{
    AgentStatus, AgentTarget, AgentWait, Emittable, Focus, HerdrCommand, HerdrSessionCommand,
    MetadataSource, OutputSource, Printout, ProcessReply, ScreenFormat, Stderr, Stdout,
};
use crate::herdr::HerdrCli;
use crate::herdr::launch::NamesHerdrHarness;
use crate::observation_lifetime::{ObservationCancellation, ObservationCancellationCause};
use std::process::Command;

/// The words a typed Herdr command is spoken in, after the `herdr`
/// executable.
pub(crate) trait SpeaksHerdr {
    fn herdr_words(&self) -> Vec<String>;
}

impl SpeaksHerdr for HerdrCommand {
    fn herdr_words(&self) -> Vec<String> {
        match self {
            HerdrCommand::ListSessions => vec!["session".into(), "list".into(), "--json".into()],
            HerdrCommand::InSession(addressed) => {
                let mut words = vec![
                    "--session".to_owned(),
                    addressed.herdr_session_name.0.clone(),
                ];
                words.extend(addressed.herdr_session_command.herdr_words());
                words
            }
        }
    }
}

impl SpeaksHerdr for HerdrSessionCommand {
    fn herdr_words(&self) -> Vec<String> {
        match self {
            HerdrSessionCommand::Snapshot => vec!["api".into(), "snapshot".into()],
            HerdrSessionCommand::ClosePane(pane) => {
                vec!["pane".into(), "close".into(), pane.0.clone()]
            }
            HerdrSessionCommand::GetPane(pane) => vec!["pane".into(), "get".into(), pane.0.clone()],
            HerdrSessionCommand::ReadPaneProcesses(pane) => vec![
                "pane".into(),
                "process-info".into(),
                "--pane".into(),
                pane.0.clone(),
            ],
            HerdrSessionCommand::RunInPane(run) => vec![
                "pane".into(),
                "run".into(),
                run.herdr_pane_id.0.clone(),
                run.shell_line.0.clone(),
            ],
            HerdrSessionCommand::WaitForPaneOutput(wait) => vec![
                "pane".into(),
                "wait-output".into(),
                wait.herdr_pane_id.0.clone(),
                "--match".into(),
                wait.output_match.0.clone(),
                "--source".into(),
                wait.output_source.herdr_words().concat(),
                "--lines".into(),
                wait.line_count.0.to_string(),
                "--timeout".into(),
                wait.milliseconds.0.to_string(),
            ],
            HerdrSessionCommand::RenamePane(rename) => vec![
                "pane".into(),
                "rename".into(),
                rename.herdr_pane_id.0.clone(),
                rename.label.0.clone(),
            ],
            HerdrSessionCommand::ReportPaneMetadata(report) => {
                let mut words = vec![
                    "pane".to_owned(),
                    "report-metadata".into(),
                    report.herdr_pane_id.0.clone(),
                    "--source".into(),
                ];
                words.extend(report.metadata_source.herdr_words());
                words.extend(["--display-agent".into(), report.display_agent.0.clone()]);
                words
            }
            HerdrSessionCommand::SendKeys(keys) => {
                let mut words = vec![
                    "pane".to_owned(),
                    "send-keys".into(),
                    keys.herdr_pane_id.0.clone(),
                ];
                words.extend(keys.key_name_vector.iter().map(|key| key.0.clone()));
                words
            }
            HerdrSessionCommand::CreateWorkspace(create) => {
                let mut words = vec![
                    "workspace".to_owned(),
                    "create".into(),
                    "--cwd".into(),
                    create.working_directory.0.clone(),
                    "--env".into(),
                    format!("FLOW_LAUNCH_REQUEST_ID={}", create.launch_request_id.0),
                ];
                words.extend(create.focus.herdr_words());
                words
            }
            HerdrSessionCommand::RenameWorkspace(rename) => vec![
                "workspace".into(),
                "rename".into(),
                rename.herdr_workspace_id.0.clone(),
                rename.label.0.clone(),
            ],
            HerdrSessionCommand::GetWorkspace(workspace) => {
                vec!["workspace".into(), "get".into(), workspace.0.clone()]
            }
            HerdrSessionCommand::StartAgent(start) => {
                let mut words = vec![
                    "agent".to_owned(),
                    "start".into(),
                    start.herdr_agent_name.0.clone(),
                    "--kind".into(),
                    start.harness_kind.expected_harness().into(),
                    "--pane".into(),
                    start.herdr_pane_id.0.clone(),
                    "--timeout".into(),
                    start.milliseconds.0.to_string(),
                ];
                if let Some(executable) = &start.codex_executable_option {
                    words.extend(["--executable".into(), executable.0.clone()]);
                }
                words.push("--".into());
                words.extend(start.harness_command_line.0.iter().cloned());
                words
            }
            HerdrSessionCommand::WaitForAgent(wait) => {
                let mut words = vec!["agent".to_owned(), "wait".into()];
                words.extend(wait.agent_target.herdr_words());
                words.extend(wait.agent_wait.herdr_words());
                words
            }
            HerdrSessionCommand::GetAgent(target) => {
                let mut words = vec!["agent".to_owned(), "get".into()];
                words.extend(target.herdr_words());
                words
            }
            HerdrSessionCommand::ReadAgent(read) => {
                let mut words = vec![
                    "agent".to_owned(),
                    "read".into(),
                    read.herdr_pane_id.0.clone(),
                    "--source".into(),
                ];
                words.extend(read.output_source.herdr_words());
                words.extend(["--lines".into(), read.line_count.0.to_string()]);
                words.push("--format".into());
                words.extend(read.screen_format.herdr_words());
                words
            }
            HerdrSessionCommand::PromptAgent(prompt) => {
                let mut words = vec![
                    "agent".to_owned(),
                    "prompt".into(),
                    prompt.herdr_pane_id.0.clone(),
                    prompt.prompt_text.0.clone(),
                ];
                if let Some(wait) = &prompt.agent_wait_option {
                    words.push("--wait".into());
                    words.extend(wait.herdr_words());
                }
                words
            }
        }
    }
}

impl SpeaksHerdr for OutputSource {
    fn herdr_words(&self) -> Vec<String> {
        vec![
            match self {
                OutputSource::Visible => "visible",
                OutputSource::RecentUnwrapped => "recent-unwrapped",
            }
            .into(),
        ]
    }
}

impl SpeaksHerdr for MetadataSource {
    fn herdr_words(&self) -> Vec<String> {
        match self {
            MetadataSource::Flow => vec!["flow".into()],
        }
    }
}

impl SpeaksHerdr for Focus {
    fn herdr_words(&self) -> Vec<String> {
        match self {
            Focus::Focused => Vec::new(),
            Focus::Unfocused => vec!["--no-focus".into()],
        }
    }
}

impl SpeaksHerdr for ScreenFormat {
    fn herdr_words(&self) -> Vec<String> {
        match self {
            ScreenFormat::Ansi => vec!["ansi".into()],
        }
    }
}

impl SpeaksHerdr for AgentTarget {
    fn herdr_words(&self) -> Vec<String> {
        match self {
            AgentTarget::Pane(pane) => vec![pane.0.clone()],
            AgentTarget::Agent(agent) => vec![agent.0.clone()],
        }
    }
}

impl SpeaksHerdr for AgentStatus {
    fn herdr_words(&self) -> Vec<String> {
        vec![
            match self {
                AgentStatus::Working => "working",
                AgentStatus::Idle => "idle",
                AgentStatus::Done => "done",
                AgentStatus::Blocked => "blocked",
            }
            .into(),
        ]
    }
}

impl SpeaksHerdr for AgentWait {
    fn herdr_words(&self) -> Vec<String> {
        let mut words = Vec::new();
        for status in &self.agent_status_vector {
            words.push("--until".to_owned());
            words.extend(status.herdr_words());
        }
        words.extend(["--timeout".into(), self.milliseconds.0.to_string()]);
        words
    }
}

/// The whole command line a Herdr command runs as: the executable, then its
/// words. A rehearsal shows it; an emission runs it.
pub(crate) trait WritesHerdrCommandLine {
    fn command_line(&self, command: &HerdrCommand) -> Vec<String>;
}

impl WritesHerdrCommandLine for HerdrCli {
    fn command_line(&self, command: &HerdrCommand) -> Vec<String> {
        std::iter::once(self.executable.to_string_lossy().into_owned())
            .chain(command.herdr_words())
            .collect()
    }
}

/// One `herdr` process: its words, its status and what it printed.
impl Emittable<HerdrCli> for HerdrCommand {
    fn emit(&self, herdr: &HerdrCli) -> ProcessReply {
        match Command::new(&herdr.executable)
            .args(self.herdr_words())
            .output()
        {
            Err(error) => ProcessReply::Unstarted(error.to_string()),
            Ok(output) => {
                let printout = Printout {
                    stdout: Stdout(String::from_utf8_lossy(&output.stdout).into_owned()),
                    stderr: Stderr(String::from_utf8_lossy(&output.stderr).into_owned()),
                };
                if output.status.success() {
                    ProcessReply::Succeeded(printout)
                } else {
                    ProcessReply::Failed(printout)
                }
            }
        }
    }
}

/// Herdr spoken on behalf of an observation that may be cancelled: the
/// process's stdout is waited on beside the cancellation, and a cancelled
/// call's child is killed and reaped before the call answers.
pub(crate) struct CancellableHerdr<'observation> {
    pub(crate) herdr: &'observation HerdrCli,
    pub(crate) cancellation: &'observation ObservationCancellation,
}

impl Emittable<CancellableHerdr<'_>> for HerdrCommand {
    fn emit(&self, translator: &CancellableHerdr<'_>) -> ProcessReply {
        use crate::event_wait::{AwaitsDescriptors, OpensExitDescriptor};
        use crate::observation_lifetime::CancelsObservation;
        use std::io::Read;
        use std::os::fd::AsFd;
        let cancellation = translator.cancellation;
        if cancellation.cause().is_some() {
            return ProcessReply::Cancelled;
        }
        let mut child = match Command::new(&translator.herdr.executable)
            .args(self.herdr_words())
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
        {
            Ok(child) => child,
            Err(error) => return ProcessReply::Unstarted(error.to_string()),
        };
        let exited = child.id().exit_descriptor().ok();
        let cancelled = |child: &mut std::process::Child| {
            let _ = child.kill();
            if let Some(exited) = &exited {
                let _ = [exited.as_fd()].ready(None);
            }
            let _ = child.wait();
            ProcessReply::Cancelled
        };
        let mut output = Vec::new();
        if let Some(mut stdout) = child.stdout.take() {
            let mut buffer = [0_u8; 8192];
            while let Ok(ready) = [stdout.as_fd(), cancellation.descriptor()].ready(None) {
                if ready[1] || cancellation.cause().is_some() {
                    return cancelled(&mut child);
                }
                match stdout.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(count) => output.extend_from_slice(&buffer[..count]),
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(_) => break,
                }
            }
        }
        // The child closed its stdout; its exit is awaited with the
        // cancellation, so a child that lingers is still killed and reaped.
        if let Some(exited) = &exited
            && let Ok(ready) = [exited.as_fd(), cancellation.descriptor()].ready(None)
            && ready[1]
            && !ready[0]
        {
            return cancelled(&mut child);
        }
        let printout = Printout {
            stdout: Stdout(String::from_utf8_lossy(&output).into_owned()),
            stderr: Stderr(String::new()),
        };
        match child.wait() {
            Ok(status) if status.success() => ProcessReply::Succeeded(printout),
            Ok(_) => ProcessReply::Failed(printout),
            Err(error) => ProcessReply::Unstarted(error.to_string()),
        }
    }
}

/// What a process reply says, read the ways Flow reads Herdr's.
pub(crate) trait ReadsProcessReply {
    /// Whether the process ran and succeeded.
    fn succeeded(&self) -> bool;
    /// The JSON object a successful process printed.
    fn json(&self) -> Result<serde_json::Value, String>;
    /// That the process ran and succeeded, or why not.
    fn status(&self) -> Result<(), String>;
    /// What a successful process printed; None for any other reply.
    fn printed(&self) -> Option<&str>;
    /// A cancellable reply as its observation reads it: what a successful
    /// process printed, None for one that did not run or failed, and the
    /// cancellation's cause for a cancelled one.
    fn observed(
        &self,
        cancellation: &ObservationCancellation,
    ) -> Result<Option<&str>, ObservationCancellationCause>;
}

impl ReadsProcessReply for ProcessReply {
    fn succeeded(&self) -> bool {
        matches!(self, ProcessReply::Succeeded(_))
    }

    fn json(&self) -> Result<serde_json::Value, String> {
        self.status()?;
        serde_json::from_str(self.printed().unwrap_or_default())
            .map_err(|error| format!("Herdr returned unreadable JSON: {error}"))
    }

    fn status(&self) -> Result<(), String> {
        match self {
            ProcessReply::Succeeded(_) => Ok(()),
            ProcessReply::Unstarted(error) => {
                Err(format!("Herdr command could not start: {error}"))
            }
            ProcessReply::Failed(printout) => Err(format!(
                "Herdr command refused the launch stage: {}",
                printout.stderr.0.trim()
            )),
            ProcessReply::Cancelled => Err("Herdr command was cancelled".into()),
        }
    }

    fn printed(&self) -> Option<&str> {
        match self {
            ProcessReply::Succeeded(printout) => Some(printout.stdout.0.as_str()),
            ProcessReply::Unstarted(_) | ProcessReply::Failed(_) | ProcessReply::Cancelled => None,
        }
    }

    fn observed(
        &self,
        cancellation: &ObservationCancellation,
    ) -> Result<Option<&str>, ObservationCancellationCause> {
        use crate::observation_lifetime::CancelsObservation;
        match self {
            ProcessReply::Cancelled => Err(cancellation
                .cause()
                .unwrap_or(ObservationCancellationCause::NexusStopped)),
            _ => Ok(self.printed()),
        }
    }
}

/// A Herdr command addressed to one Herdr session.
pub(crate) trait AddressesHerdrSession {
    fn in_session(
        herdr_session_name: &signal_flow::HerdrSessionName,
        herdr_session_command: HerdrSessionCommand,
    ) -> Self;
}

impl AddressesHerdrSession for HerdrCommand {
    fn in_session(
        herdr_session_name: &signal_flow::HerdrSessionName,
        herdr_session_command: HerdrSessionCommand,
    ) -> Self {
        HerdrCommand::InSession(crate::generated::emission::InSession_Data {
            herdr_session_name: herdr_session_name.clone(),
            herdr_session_command,
        })
    }
}
