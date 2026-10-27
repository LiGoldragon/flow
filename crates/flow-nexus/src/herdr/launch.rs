//! Staged native launch through Herdr's documented CLI surface.
//!
//! Launching, claiming an identity, registering it, submitting each part of
//! the first USER message, and observing its target-side receipt are
//! separate operations. The adapter never chooses registration policy and
//! never retries a part whose write may already have succeeded. A part is
//! typed through Herdr's socket API, one JSON request line, never as a
//! process argument.

use super::{DecodesFlowClaim, FlowClaim, HerdrCli, claude_paste};
use crate::codex::NamesBoundCodexThread;
use crate::codex::SelectsCodexEndpoint;
pub use crate::generated::starter::StartupProgress;
use crate::generated::starter::{
    DeliveryIntended, JsonRow, LeanStart, PaneBound, PromptsThroughHerdrSocket, ReadsStartupRecord,
    ReadsTypedUserText, RendersLeanStart, StartupReading, Textual,
};
use crate::herdr::reservation::ChoosesNativeSession;
use crate::title::{NativeTitle, ShowsNativeTitle, TitlesFlow};
use sha2::{Digest, Sha256};
use signal_flow::{
    ComposedLaunch, HarnessKind, HerdrPaneBinding, NativeLaunchBinding, NativeLaunchFailure,
    NativeTargetReceipt, NativeTranscriptAbsence, NativeTranscriptBoundary, NativeTranscriptCursor,
    PromptDeliveryIntent, PromptDeliveryResult, RegistrationAcknowledgement,
};
use std::{
    fs::{self, File, Metadata},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    os::unix::{fs::MetadataExt, net::UnixStream},
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

/// Creates topology without launching a harness or sending a prompt.
pub trait CreatesHerdrLaunchPane {
    fn create_launch_pane(&self, launch: &ComposedLaunch) -> Result<HerdrPaneBinding, String>;
}

/// Starts the selected native interactive harness without a positional prompt.
/// `reserved` is the FlowId Reserve claimed for this launch, if any: the
/// harness is started with it as FLOW_ID in its environment and with the
/// native session id it was claimed for. Without one, the harness starts
/// with no FLOW_ID and chooses its own session.
pub trait StartsNativeHerdrHarness {
    fn start_native_harness(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
        reserved: Option<&str>,
    ) -> Result<(), SpawnFailure>;
    /// Starts the Codex TUI attached to the thread Flow already opened and
    /// named, idle with no turn yet: `resume <thread>` after the launch's
    /// switches.
    fn attach_codex_harness(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
        thread: &str,
    ) -> Result<(), SpawnFailure>;
}

/// How a harness is told its native session at spawn.
enum ClaudeSession {
    /// The harness chooses its own session.
    Unchosen,
    /// The session Flow chose for a reserved Claude launch: `--session-id`.
    Chosen(String),
    /// The Codex thread Flow opened through the app-server, which the TUI
    /// attaches to: `resume <thread>`.
    Attached(String),
}

/// The one Herdr `agent start` every launch runs.
trait SpawnsNativeHarness {
    fn spawn_native_harness(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
        flow_id: Option<&str>,
        session: ClaudeSession,
    ) -> Result<(), SpawnFailure>;
}

/// Observes an official Herdr integration report and binds it to an exact
/// native identity claim. Screen detection and caller metadata are rejected.
/// With a `reserved` FlowId, the observed native session must be the one
/// Flow chose and the claim must name that FlowId.
pub trait ObservesNativeLaunchBinding {
    fn observe_native_binding(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
        reserved: Option<&str>,
    ) -> Result<NativeLaunchBinding, String>;
    /// The binding of a Codex TUI attached to the thread Flow opened: the
    /// session Herdr's integration reports must be that thread.
    fn observe_attached_codex_binding(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
        thread: &str,
    ) -> Result<NativeLaunchBinding, String>;
}

/// Why Herdr's `agent get` of a launch pane is not the bound native target.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum NativeTargetRefusal {
    #[error("{0}")]
    HerdrUnreadable(String),
    #[error("receipt target no longer matches the registered Herdr pane")]
    PaneMismatch,
    #[error("receipt target has no official native session")]
    NoNativeSession,
    #[error("receipt target native identity is no longer exact")]
    IdentityInexact,
}

impl From<NativeTargetRefusal> for String {
    fn from(refusal: NativeTargetRefusal) -> Self {
        refusal.to_string()
    }
}

/// The stage at which a launch registration was refused. Each variant is a
/// fixed diagnostic naming the stage; the one adapter text it carries is the
/// transcript's, which holds no secret.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RegistrationRefusal {
    #[error("the Herdr pane binding does not belong to this launch request")]
    PaneBindingForeign,
    #[error("the registration acknowledgement does not match the native binding")]
    AcknowledgementMismatch,
    #[error("the composed first prompt is not canonical")]
    PromptNotCanonical,
    #[error("the native target is not the thread bound: {0}")]
    NativeTarget(#[from] NativeTargetRefusal),
    #[error("the native transcript boundary could not be captured: {0}")]
    TranscriptBoundary(String),
    #[error("the native transcript does not begin by naming the opened thread")]
    ThreadNotNamed,
}

impl From<&RegistrationRefusal> for signal_flow::RegistrationRefusal {
    fn from(refusal: &RegistrationRefusal) -> Self {
        match refusal {
            RegistrationRefusal::PaneBindingForeign => Self::PaneBindingForeign,
            RegistrationRefusal::AcknowledgementMismatch => Self::AcknowledgementMismatch,
            RegistrationRefusal::PromptNotCanonical => Self::PromptNotCanonical,
            RegistrationRefusal::NativeTarget(target) => Self::NativeTarget(target.into()),
            RegistrationRefusal::TranscriptBoundary(_) => Self::TranscriptBoundary,
            RegistrationRefusal::ThreadNotNamed => Self::ThreadNotNamed,
        }
    }
}

impl From<&NativeTargetRefusal> for signal_flow::NativeTargetRefusal {
    fn from(refusal: &NativeTargetRefusal) -> Self {
        match refusal {
            NativeTargetRefusal::HerdrUnreadable(_) => Self::HerdrUnreadable,
            NativeTargetRefusal::PaneMismatch => Self::PaneMismatch,
            NativeTargetRefusal::NoNativeSession => Self::NoNativeSession,
            NativeTargetRefusal::IdentityInexact => Self::IdentityInexact,
        }
    }
}

/// Why a launch's native transcript was not read as its receipt: the pane
/// is not the bound native target, the transcript could not be read (the
/// adapter's text, which holds no secret, kept for the log), or what it
/// shows contradicts the launch.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TranscriptRefusal {
    #[error("{0}")]
    Target(#[from] NativeTargetRefusal),
    #[error("{0}")]
    Unreadable(String),
    #[error("the native transcript contradicts the launch: {0:?}")]
    Contradicted(signal_flow::NativeLaunchFailure),
}

impl From<String> for TranscriptRefusal {
    fn from(error: String) -> Self {
        Self::Unreadable(error)
    }
}

impl From<signal_flow::NativeLaunchFailure> for TranscriptRefusal {
    fn from(failure: signal_flow::NativeLaunchFailure) -> Self {
        Self::Contradicted(failure)
    }
}

impl From<TranscriptRefusal> for String {
    fn from(refusal: TranscriptRefusal) -> Self {
        refusal.to_string()
    }
}

impl From<&TranscriptRefusal> for signal_flow::NativeLaunchFailure {
    fn from(refusal: &TranscriptRefusal) -> Self {
        match refusal {
            TranscriptRefusal::Target(target) => target.into(),
            TranscriptRefusal::Unreadable(_) => Self::TranscriptUnreadable,
            TranscriptRefusal::Contradicted(failure) => failure.clone(),
        }
    }
}

/// Whether a Codex rollout row is the session meta of exactly this thread.
pub(crate) trait NamesCodexThread {
    fn names_thread(&self, thread: &str) -> bool;
}

impl NamesCodexThread for serde_json::Value {
    fn names_thread(&self, thread: &str) -> bool {
        self.get("type").and_then(serde_json::Value::as_str) == Some("session_meta")
            && self
                .pointer("/payload/id")
                .and_then(serde_json::Value::as_str)
                == Some(thread)
    }
}

/// Why a launched Flow was not given its title. Each variant is a fixed
/// diagnostic; adapter text stays in `HerdrUnreadable`'s display and is
/// never logged.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TitleRefusal {
    #[error("native binding does not belong to this launch")]
    ForeignBinding,
    #[error(transparent)]
    Title(#[from] crate::title::TitleRefused),
    #[error(transparent)]
    Target(#[from] NativeTargetRefusal),
    #[error("Claude target did not become interactively ready for its title")]
    NotReady,
    #[error("the native transcript title is unreadable")]
    TranscriptUnreadable,
    #[error("Claude native title readback differs from the set title")]
    ReadbackDiffers,
    #[error("the Codex app-server refused the thread name")]
    CodexNamingRefused,
    #[error("Herdr pane label readback differs from the title")]
    PaneLabelRefused,
    #[error("Herdr workspace label readback differs from the title")]
    WorkspaceLabelRefused,
}

/// After the Flow ID is claimed, sets the canonical native title, then gives
/// the title to every Herdr surface the living sees: the pane label, the
/// pane's displayed agent, and the workspace label. Each is read back before
/// any prompt is authorized. The Herdr agent name stays the internal
/// transport key in the binding and is shown nowhere.
pub trait TitlesNativeFlow {
    fn title_native_flow(
        &self,
        launch: &ComposedLaunch,
        binding: &NativeLaunchBinding,
    ) -> Result<NativeTitle, TitleRefusal>;
}

/// Converts an exact registration acknowledgement into the value that the
/// integration layer must durably store before prompt submission.
pub trait AcceptsLaunchRegistration {
    fn accept_registration(
        &self,
        launch: &ComposedLaunch,
        binding: &NativeLaunchBinding,
        acknowledgement: &RegistrationAcknowledgement,
    ) -> Result<PromptDeliveryIntent, RegistrationRefusal>;
}

/// Types the part of the first USER message the durable intent last
/// counted as submitted, once per invocation, through Herdr's socket. The
/// caller must durably gate invocation; a successful Herdr write remains
/// ambiguous until the native transcript independently shows the part.
pub trait SubmitsStartupPart {
    fn submit_startup_part(
        &self,
        launch: &ComposedLaunch,
        durable_intent: &PromptDeliveryIntent,
    ) -> Result<PromptDeliveryResult, String>;
}

/// Reads the native transcript of the bound session for the parts of the
/// first USER message, each authenticated by its digest, in order, and the
/// flow's first answer after the last. A turn that contradicts them refuses.
pub trait ObservesNativeTargetReceipt {
    fn observe_native_target_receipt(
        &self,
        durable_intent: &PromptDeliveryIntent,
    ) -> Result<StartupProgress, TranscriptRefusal>;
}

/// The Codex TUI's terminal title items: the thread title alone. The items
/// codex-tui 0.161.0-alpha.2 accepts in `[tui].terminal_title` include
/// `project-name`, `current-dir`, `run-state` and `thread-title`.
const CODEX_TERMINAL_TITLE: &str = "tui.terminal_title=[\"thread-title\"]";

/// How each Codex block a launch may leave out opens when Codex puts it in a
/// request: the openings of the developer and user context messages in the
/// rollout codex-tui 0.161.0-alpha.2 wrote for thread
/// 01a146e1-1825-7753-9a4d-2107ac3c0f24 (its first request carried every
/// one of them), all absent from thread 01a146e4-10dd-7560-a685-30a9896d87f3
/// opened with the AGENTS.md and skill-listing exclusions.
const CODEX_LEFT_OUT_OPENINGS: &[(signal_flow::LeaveOut, &str)] = &[
    (
        signal_flow::LeaveOut::CodexAgentsMd,
        "# AGENTS.md instructions for ",
    ),
    (
        signal_flow::LeaveOut::CodexSkillListing,
        "<skills_instructions>",
    ),
    (
        signal_flow::LeaveOut::CodexPermissions,
        "<permissions instructions>",
    ),
    (
        signal_flow::LeaveOut::CodexCollaborationMode,
        "<collaboration_mode>",
    ),
    (
        signal_flow::LeaveOut::CodexEnvironmentContext,
        "<environment_context>",
    ),
    // The multi-agent role and mode messages of thread
    // 01a14773-f58c-7ca1-978b-1ac675d19399 (gpt-6-luna), which the
    // developer-instructions leave-out removes through their hints.
    (
        signal_flow::LeaveOut::CodexDeveloperInstructions,
        "<multi_agent_role>",
    ),
    (
        signal_flow::LeaveOut::CodexDeveloperInstructions,
        "<multi_agent_mode>",
    ),
];

/// Herdr's socket API, spoken directly: one JSON request line out, one JSON
/// reply line back, on the socket `herdr session list` names for the pane's
/// session. Herdr 0.8.2 reads a request line up to 1 MiB.
/// Herdr's `agent.prompt` for the pane's agent types the text, then Enter;
/// its reply is the JSON value Herdr answered with a result.
impl PromptsThroughHerdrSocket for HerdrCli {
    fn herdr_session_socket<N: Textual>(&self, input: N) -> Result<String, String> {
        use crate::caller::KnowsHerdrServers;
        let herdr_session_name = input.text();
        self.herdr_sessions()
            .ok_or_else(|| "Herdr sessions could not be listed".to_owned())?
            .into_iter()
            .find(|session| session.name == herdr_session_name)
            .map(|session| session.socket.display().to_string())
            .ok_or_else(|| "Herdr lists no socket for the pane's session".to_owned())
    }

    fn prompt_through_socket<N: PaneBound, O: Textual>(
        &self,
        input_0: N,
        input_1: O,
    ) -> Result<serde_json::Value, String> {
        let pane = input_0.pane();
        let text = input_1.text();
        let request = serde_json::json!({
            "id": format!("flow:prompt:{}", pane.launch_request_id),
            "method": "agent.prompt",
            "params": { "target": pane.herdr_pane_id, "text": text }
        });
        let mut line = serde_json::to_string(&request)
            .map_err(|error| format!("Herdr prompt request not encoded: {error}"))?;
        line.push('\n');
        if line.len() > self.limits.herdr_line as usize {
            return Err("Herdr prompt request exceeds its line limit".into());
        }
        let socket = self.herdr_session_socket(&pane.herdr_session_name)?;
        let mut stream = UnixStream::connect(&socket)
            .map_err(|error| format!("Herdr socket unreachable: {error}"))?;
        stream
            .set_read_timeout(Some(Duration::from_millis(
                self.timings.prompt_reply_timeout as u64,
            )))
            .and_then(|_| {
                stream.set_write_timeout(Some(Duration::from_millis(
                    self.timings.prompt_reply_timeout as u64,
                )))
            })
            .map_err(|error| error.to_string())?;
        stream
            .write_all(line.as_bytes())
            .and_then(|_| stream.flush())
            .map_err(|error| format!("Herdr prompt request not written: {error}"))?;
        let mut reply = String::new();
        BufReader::new(&stream)
            .read_line(&mut reply)
            .map_err(|error| format!("Herdr prompt reply not read: {error}"))?;
        let reply: serde_json::Value = serde_json::from_str(reply.trim())
            .map_err(|error| format!("Herdr returned unreadable JSON: {error}"))?;
        if let Some(error) = reply.get("error") {
            return Err(format!("Herdr refused the prompt: {error}"));
        }
        if reply.get("result").is_none() {
            return Err("Herdr prompt reply carries no result".into());
        }
        Ok(reply)
    }
}

pub(crate) trait PreparesClaudePane {
    const CLAUDE_CHILD_SESSION_ENVIRONMENT: &'static str = "CLAUDE_CODE_CHILD_SESSION";
    /// Inherited Claude identity that a new Flow must not carry. A shared
    /// `CLAUDE_JOB_DIR` shares one job state between processes: a new
    /// session adopts the title another session left there, and its own
    /// `/rename` is propagated to every process on that job directory.
    /// `FLOW_ID` is among them: the harness hook reports under the FLOW_ID
    /// in its environment, and a new flow must never report as another.
    const CLAUDE_INHERITED_ENVIRONMENT: [&'static str; 5] = [
        Self::CLAUDE_CHILD_SESSION_ENVIRONMENT,
        "CLAUDE_JOB_DIR",
        "CLAUDE_CODE_SESSION_ID",
        "CLAUDE_CODE_SESSION_KIND",
        "FLOW_ID",
    ];
    /// The permission-skipping flag. The installed `claude` (CriomOS-home's
    /// claude-code package) already execs `.claude-wrapped
    /// --dangerously-skip-permissions "$@"`, and 88475f's pane showed it
    /// twice when Flow passed it as well. Flow therefore never passes it:
    /// the launch's own permission mode is carried by the flag settings
    /// below, and the flag, when present, comes once, from the package.
    #[cfg(test)]
    const CLAUDE_SKIP_PERMISSIONS_FLAG: &'static str = "--dangerously-skip-permissions";
    /// Claude offers "Make auto mode your default permission mode?" when the
    /// user settings name a non-auto default mode and no other settings
    /// source names one. Flag settings are such a source: naming the mode the
    /// launch already runs in suppresses the offer without touching any
    /// settings file. They also carry the harness hook: `flow-hook` on
    /// SessionStart, every PostToolUse, UserPromptSubmit, Stop and
    /// SessionEnd, so each harness event reaches Flow as a Report: a Stop is
    /// the flow's turn end, a submitted prompt the receipt of a queued
    /// request, and a session's end the flow's exit.
    /// A launch that leaves out the attribution reminder sets the
    /// attribution lines empty here.
    fn claude_flag_settings(&self, lean: &LeanStart) -> String;
    const CLAUDE_REMOTE_CONTROL_FLAG: &'static str = "--remote-control";
    /// Claude Code 2.1.294's session name: "Set a display name for this
    /// session (shown in the prompt box, /resume picker, and terminal
    /// title)". A headless probe with `--disable-slash-commands` recorded it
    /// as the same `custom-title` and `agent-name` transcript records
    /// `/rename` writes.
    const CLAUDE_NAME_FLAG: &'static str = "--name";
    fn prepare_claude_pane_environment(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
        reserved: Option<&str>,
    ) -> Result<(), String>;
}

impl PreparesClaudePane for HerdrCli {
    fn claude_flag_settings(&self, lean: &LeanStart) -> String {
        let hook = serde_json::json!([{
            "hooks": [{
                "type": "command",
                "command": self.harness_hook.to_string_lossy(),
            }]
        }]);
        let every_tool = serde_json::json!([{
            "matcher": "*",
            "hooks": [{
                "type": "command",
                "command": self.harness_hook.to_string_lossy(),
            }]
        }]);
        let mut settings = serde_json::json!({
            "permissions": { "defaultMode": "bypassPermissions" },
            "hooks": {
                "SessionStart": hook,
                "PostToolUse": every_tool,
                "UserPromptSubmit": hook,
                "Stop": hook,
                "SessionEnd": hook,
            }
        });
        if lean.attribution_off() {
            settings["attribution"] = serde_json::json!({ "commit": "", "pr": "" });
        }
        settings.to_string()
    }

    fn prepare_claude_pane_environment(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
        reserved: Option<&str>,
    ) -> Result<(), String> {
        let marker_suffix = format!(
            "{:x}",
            Sha256::digest(
                format!(
                    "flow-claude-environment-v1\0{}",
                    launch.launch_profile.launch_request_id
                )
                .as_bytes()
            )
        );
        let marker = format!("FLOW_CLAUDE_ENV_READY_{marker_suffix}");
        self.run_status(&[
            "--session".into(),
            pane.herdr_session_name.clone(),
            "pane".into(),
            "run".into(),
            pane.herdr_pane_id.clone(),
            marker_suffix.claude_environment_preparation(
                reserved,
                &self.ordinary_socket,
                &self.executable,
                &pane.herdr_session_name,
                &launch.lean_start().environment(),
            ),
        ])?;
        let response = self.run_json(&[
            "--session".into(),
            pane.herdr_session_name.clone(),
            "pane".into(),
            "wait-output".into(),
            pane.herdr_pane_id.clone(),
            "--match".into(),
            marker.clone(),
            "--source".into(),
            "recent-unwrapped".into(),
            "--lines".into(),
            self.limits.output_lines.to_string(),
            "--timeout".into(),
            self.timings.environment_ready_timeout.to_string(),
        ])?;
        let observed_pane = response
            .pointer("/result/pane_id")
            .and_then(serde_json::Value::as_str);
        let matched_line = response
            .pointer("/result/matched_line")
            .and_then(serde_json::Value::as_str);
        if observed_pane != Some(pane.herdr_pane_id.as_str())
            || matched_line != Some(marker.as_str())
        {
            return Err(
                "Herdr did not prove the Claude environment was prepared in the launch pane".into(),
            );
        }
        Ok(())
    }
}

pub(crate) trait RunsHerdr {
    fn run_json(&self, arguments: &[String]) -> Result<serde_json::Value, String>;
    fn run_status(&self, arguments: &[String]) -> Result<(), String>;
}

impl RunsHerdr for HerdrCli {
    fn run_json(&self, arguments: &[String]) -> Result<serde_json::Value, String> {
        let output = Command::new(&self.executable)
            .args(arguments)
            .output()
            .map_err(|error| format!("Herdr command could not start: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "Herdr command refused the launch stage: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        serde_json::from_slice(&output.stdout)
            .map_err(|error| format!("Herdr returned unreadable JSON: {error}"))
    }

    fn run_status(&self, arguments: &[String]) -> Result<(), String> {
        let output = Command::new(&self.executable)
            .args(arguments)
            .output()
            .map_err(|error| format!("Herdr command could not start: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "Herdr command refused the launch stage: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Ok(())
    }
}

pub(crate) trait ClaimsNativeIdentity {
    fn canonical_native_identity(native_session_id: &str) -> String;
    fn configured_workspace_root(&self) -> Result<&Path, String>;
    fn claimed_native_identity(
        &self,
        flow_id: &str,
        harness: &HarnessKind,
    ) -> Result<String, String>;
    fn claim_flow_identity(
        &self,
        harness: &HarnessKind,
        native_session_id: &str,
    ) -> Result<String, String>;
    fn verify_native_target(
        &self,
        pane: &HerdrPaneBinding,
        native_session_id: &str,
        harness: &HarnessKind,
    ) -> Result<serde_json::Value, NativeTargetRefusal>;
}

impl ClaimsNativeIdentity for HerdrCli {
    fn canonical_native_identity(native_session_id: &str) -> String {
        native_session_id
            .bytes()
            .filter(|byte| *byte != b'-')
            .map(char::from)
            .collect::<String>()
            .to_ascii_lowercase()
    }

    fn configured_workspace_root(&self) -> Result<&Path, String> {
        if !self.flows_root.is_absolute()
            || self.flows_root.file_name().and_then(|name| name.to_str()) != Some("flows")
        {
            return Err("configured Flow root must be an absolute directory named flows".into());
        }
        let root = self
            .flows_root
            .parent()
            .filter(|root| *root != Path::new("/"))
            .ok_or_else(|| "configured Flow root has no bounded workspace parent".to_owned())?;
        if !root.is_dir() {
            return Err("configured workspace root is not a directory".into());
        }
        Ok(root)
    }

    fn claimed_native_identity(
        &self,
        flow_id: &str,
        harness: &HarnessKind,
    ) -> Result<String, String> {
        let marker_path = self.flows_root.join(format!(".{flow_id}.flow-id"));
        let metadata = fs::symlink_metadata(&marker_path)
            .map_err(|_| "native identity claim marker is absent".to_owned())?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err("native identity claim marker is not a regular file".into());
        }
        let marker = fs::read_to_string(marker_path)
            .map_err(|error| format!("native identity claim is unreadable: {error}"))?;
        let claim = FlowClaim::decode(&marker)
            .ok_or_else(|| "native identity claim is invalid".to_owned())?;
        if claim.alias != flow_id || &claim.harness_kind != harness {
            return Err("native identity claim does not match flow or harness".into());
        }
        Ok(claim.identity)
    }

    fn claim_flow_identity(
        &self,
        harness: &HarnessKind,
        native_session_id: &str,
    ) -> Result<String, String> {
        let harness_name = harness.expected_harness();
        let mut command = Command::new(&self.flow_id_executable);
        command
            .arg(harness_name)
            .arg("--flows-root")
            .arg(&self.flows_root);
        match harness {
            HarnessKind::Codex => {
                command.env("CODEX_SESSION_ID", native_session_id);
            }
            HarnessKind::Claude => {
                command.arg("--parent-session").arg(native_session_id);
            }
        }
        let output = command
            .output()
            .map_err(|error| format!("flow-id claim helper could not start: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "flow-id claim helper refused the native identity: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        let flow_id = String::from_utf8(output.stdout)
            .map_err(|_| "flow-id claim helper returned non-UTF-8 output".to_owned())?;
        let flow_id = flow_id.trim_end_matches(['\r', '\n']);
        if flow_id.is_empty()
            || flow_id.contains(char::is_whitespace)
            || !flow_id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err("flow-id claim helper returned an invalid alias".into());
        }
        Ok(flow_id.into())
    }

    fn verify_native_target(
        &self,
        pane: &HerdrPaneBinding,
        native_session_id: &str,
        harness: &HarnessKind,
    ) -> Result<serde_json::Value, NativeTargetRefusal> {
        // The pane id is the target: the flow may have renamed its agent
        // since launch, and the name is read, never matched.
        let response = self
            .run_json(&[
                "--session".into(),
                pane.herdr_session_name.clone(),
                "agent".into(),
                "get".into(),
                pane.herdr_pane_id.clone(),
            ])
            .map_err(NativeTargetRefusal::HerdrUnreadable)?;
        let agent = response.pointer("/result/agent").ok_or_else(|| {
            NativeTargetRefusal::HerdrUnreadable(
                "Herdr agent get returned no receipt target".into(),
            )
        })?;
        if agent
            .get("workspace_id")
            .and_then(serde_json::Value::as_str)
            != Some(pane.herdr_workspace_id.as_str())
            || agent.get("pane_id").and_then(serde_json::Value::as_str)
                != Some(pane.herdr_pane_id.as_str())
            || agent.get("terminal_id").and_then(serde_json::Value::as_str)
                != Some(pane.herdr_terminal_id.as_str())
        {
            return Err(NativeTargetRefusal::PaneMismatch);
        }
        let expected_harness = harness.expected_harness();
        if agent.get("agent").and_then(serde_json::Value::as_str) != Some(expected_harness) {
            return Err(NativeTargetRefusal::IdentityInexact);
        }
        // A remote-resumed Codex TUI reports no agent session: its target is
        // the thread Flow opened plus the recognized pane.  A Flow-owned
        // Claude is likewise outside Herdr's managed-agent lifecycle.  Herdr
        // reports its pane state but no `agent_session`; in that case the
        // exact process argv is the session authority.
        let session = match (agent.get("agent_session"), harness) {
            (Some(session), _) => session,
            (None, HarnessKind::Codex) => return Ok(agent.clone()),
            (None, HarnessKind::Claude) => {
                if agent
                    .get("agent_status")
                    .and_then(serde_json::Value::as_str)
                    != Some("idle")
                {
                    return Err(NativeTargetRefusal::IdentityInexact);
                }
                let response = self
                    .run_json(&[
                        "--session".into(),
                        pane.herdr_session_name.clone(),
                        "pane".into(),
                        "process-info".into(),
                        "--pane".into(),
                        pane.herdr_pane_id.clone(),
                    ])
                    .map_err(NativeTargetRefusal::HerdrUnreadable)?;
                let matches_session = response
                    .pointer("/result/process_info/foreground_processes")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(|process| process.get("argv").and_then(serde_json::Value::as_array))
                    .any(|argv| {
                        argv.windows(2).any(|pair| {
                            pair[0].as_str() == Some("--session-id")
                                && pair[1].as_str() == Some(native_session_id)
                        })
                    });
                return matches_session
                    .then(|| agent.clone())
                    .ok_or(NativeTargetRefusal::IdentityInexact);
            }
        };
        let expected_source = match harness {
            HarnessKind::Claude => "flow".to_owned(),
            HarnessKind::Codex => format!("herdr:{expected_harness}"),
        };
        if session.get("source").and_then(serde_json::Value::as_str)
            != Some(expected_source.as_str())
            || session.get("agent").and_then(serde_json::Value::as_str) != Some(expected_harness)
            || session.get("kind").and_then(serde_json::Value::as_str) != Some("id")
            || session.get("value").and_then(serde_json::Value::as_str) != Some(native_session_id)
        {
            return Err(NativeTargetRefusal::IdentityInexact);
        }
        Ok(agent.clone())
    }
}

pub(crate) trait CapturesTranscripts {
    fn transcript_candidates(
        &self,
        pane: &HerdrPaneBinding,
        native_session_id: &str,
        harness: &HarnessKind,
        model_name: Option<&str>,
    ) -> Result<(PathBuf, Metadata, Vec<PathBuf>), TranscriptRefusal>;
    fn capture_transcript_boundary(
        &self,
        binding: &NativeLaunchBinding,
        model_name: &str,
    ) -> Result<NativeTranscriptBoundary, String>;
    fn capture_transcript_start_boundary(
        &self,
        binding: &NativeLaunchBinding,
        model_name: &str,
    ) -> Result<NativeTranscriptBoundary, String>;
    fn receipt_input(
        &self,
        intent: &PromptDeliveryIntent,
    ) -> Result<Option<(File, bool)>, TranscriptRefusal>;
}

impl CapturesTranscripts for HerdrCli {
    fn transcript_candidates(
        &self,
        pane: &HerdrPaneBinding,
        native_session_id: &str,
        harness: &HarnessKind,
        model_name: Option<&str>,
    ) -> Result<(PathBuf, Metadata, Vec<PathBuf>), TranscriptRefusal> {
        self.verify_native_target(pane, native_session_id, harness)?;
        let root =
            match harness {
                HarnessKind::Codex => {
                    &self
                        .codex_endpoints
                        .endpoint_for(model_name.ok_or_else(|| {
                            "Codex transcript selection requires a model".to_owned()
                        })?)
                        .map_err(|error| error.to_string())?
                        .transcript_root
                }
                HarnessKind::Claude => &self.claude_transcript_root,
            };
        let canonical_root = root.canonicalize().map_err(|error| {
            format!("configured native transcript root is unavailable: {error}")
        })?;
        let root_before = fs::metadata(&canonical_root)
            .map_err(|error| format!("native transcript root metadata failed: {error}"))?;
        if !root_before.is_dir() {
            return Err("configured native transcript root is not a directory"
                .to_owned()
                .into());
        }
        let mut found = Vec::new();
        canonical_root.collect_native_transcripts(native_session_id, harness, 0, &mut found)?;
        let root_after = fs::metadata(&canonical_root)
            .map_err(|error| format!("native transcript root metadata failed: {error}"))?;
        if root_before.dev() != root_after.dev() || root_before.ino() != root_after.ino() {
            return Err(
                "configured native transcript root changed during observation"
                    .to_owned()
                    .into(),
            );
        }
        Ok((canonical_root, root_after, found))
    }

    fn capture_transcript_boundary(
        &self,
        binding: &NativeLaunchBinding,
        model_name: &str,
    ) -> Result<NativeTranscriptBoundary, String> {
        let (root, root_metadata, found) = self.transcript_candidates(
            &binding.herdr_pane_binding,
            &binding.native_session_id,
            &binding.harness_kind,
            Some(model_name),
        )?;
        if found.is_empty() {
            return Ok(NativeTranscriptBoundary::Absent(NativeTranscriptAbsence {
                native_session_id: binding.native_session_id.clone(),
                harness_kind: binding.harness_kind.clone(),
                transcript_root_device: root_metadata.dev().to_string(),
                transcript_root_inode: root_metadata.ino().to_string(),
            }));
        }
        let transcript = root.one_resolved_transcript(found)?;
        let mut file = File::open(&transcript)
            .map_err(|error| format!("native transcript is unreadable: {error}"))?;
        let metadata = file
            .metadata()
            .map_err(|error| format!("native transcript metadata failed: {error}"))?;
        let byte_offset = metadata.len();
        let (prefix_sha256, last_byte) = file.hash_prefix(byte_offset)?;
        if last_byte.is_some_and(|byte| byte != b'\n') {
            return Err("native transcript boundary is not a complete JSONL record".into());
        }
        let metadata_after = file
            .metadata()
            .map_err(|error| format!("native transcript metadata failed: {error}"))?;
        if metadata.dev() != metadata_after.dev()
            || metadata.ino() != metadata_after.ino()
            || metadata_after.len() < byte_offset
        {
            return Err("native transcript changed during boundary observation".into());
        }
        Ok(NativeTranscriptBoundary::Existing(NativeTranscriptCursor {
            native_session_id: binding.native_session_id.clone(),
            harness_kind: binding.harness_kind.clone(),
            transcript_device: metadata.dev().to_string(),
            transcript_inode: metadata.ino().to_string(),
            transcript_byte_offset: i64::try_from(byte_offset)
                .map_err(|_| "native transcript boundary exceeds i64".to_owned())?,
            transcript_prefix_sha256: prefix_sha256,
        }))
    }

    fn capture_transcript_start_boundary(
        &self,
        binding: &NativeLaunchBinding,
        model_name: &str,
    ) -> Result<NativeTranscriptBoundary, String> {
        let (root, _, found) = self.transcript_candidates(
            &binding.herdr_pane_binding,
            &binding.native_session_id,
            &binding.harness_kind,
            Some(model_name),
        )?;
        let transcript = root.one_resolved_transcript(found)?;
        let file = File::open(transcript)
            .map_err(|error| format!("native transcript is unreadable: {error}"))?;
        let metadata = file
            .metadata()
            .map_err(|error| format!("native transcript metadata failed: {error}"))?;
        Ok(NativeTranscriptBoundary::Existing(NativeTranscriptCursor {
            native_session_id: binding.native_session_id.clone(),
            harness_kind: binding.harness_kind.clone(),
            transcript_device: metadata.dev().to_string(),
            transcript_inode: metadata.ino().to_string(),
            transcript_byte_offset: 0,
            transcript_prefix_sha256: format!("{:x}", Sha256::digest([])),
        }))
    }

    fn receipt_input(
        &self,
        intent: &PromptDeliveryIntent,
    ) -> Result<Option<(File, bool)>, TranscriptRefusal> {
        if !intent.boundary_matches_intent() {
            return Err(signal_flow::NativeLaunchFailure::BoundaryMismatch.into());
        }
        let (root, root_metadata, found) = self.transcript_candidates(
            &intent.herdr_pane_binding,
            &intent.native_session_id,
            &intent.harness_kind,
            Some(&intent.model_name),
        )?;
        match &intent.native_transcript_boundary {
            NativeTranscriptBoundary::Absent(absence) => {
                let expected_device = absence
                    .transcript_root_device
                    .decimal_identity("root device")?;
                let expected_inode = absence
                    .transcript_root_inode
                    .decimal_identity("root inode")?;
                if root_metadata.dev() != expected_device || root_metadata.ino() != expected_inode {
                    return Err("configured native transcript root was replaced"
                        .to_owned()
                        .into());
                }
                if found.is_empty() {
                    return Ok(None);
                }
                let transcript = root.one_resolved_transcript(found)?;
                File::open(transcript)
                    .map(|file| Some((file, true)))
                    .map_err(|error| format!("native transcript is unreadable: {error}").into())
            }
            NativeTranscriptBoundary::Existing(cursor) => {
                let transcript = root.one_resolved_transcript(found)?;
                let mut file = File::open(transcript)
                    .map_err(|error| format!("native transcript is unreadable: {error}"))?;
                let metadata = file
                    .metadata()
                    .map_err(|error| format!("native transcript metadata failed: {error}"))?;
                let expected_device = cursor.transcript_device.decimal_identity("device")?;
                let expected_inode = cursor.transcript_inode.decimal_identity("inode")?;
                let byte_offset = u64::try_from(cursor.transcript_byte_offset)
                    .map_err(|_| "native transcript boundary is negative".to_owned())?;
                if metadata.dev() != expected_device || metadata.ino() != expected_inode {
                    return Err("native transcript file was replaced".to_owned().into());
                }
                if metadata.len() < byte_offset {
                    return Err("native transcript file was truncated".to_owned().into());
                }
                let (prefix_sha256, _) = file.hash_prefix(byte_offset)?;
                if prefix_sha256 != cursor.transcript_prefix_sha256 {
                    return Err("native transcript prefix changed after intent persistence"
                        .to_owned()
                        .into());
                }
                file.seek(SeekFrom::Start(byte_offset))
                    .map_err(|error| format!("native transcript seek failed: {error}"))?;
                Ok(Some((file, false)))
            }
        }
    }
}

/// The shell line typed into a Claude launch pane before the harness: it
/// removes the inherited Claude identity, exports `FLOW_SOCKET` as the
/// ordinary socket of the Nexus launching the flow, and, for a launch
/// Reserve gave a FlowId, exports that FlowId as FLOW_ID. The harness Herdr
/// starts at that prompt, and every hook it runs, carries both, so
/// `flow-hook`'s `flow` reports to this Nexus whatever runtime directory
/// the pane's shell has. The FlowId is a claim alias, lowercase hex, so it
/// needs no quoting; the socket path is quoted for the shell.
pub(crate) trait PreparesClaudeEnvironment {
    /// `environment` is the launch's leave-out variables, each NAME=value.
    fn claude_environment_preparation(
        &self,
        reserved: Option<&str>,
        socket: &Path,
        herdr: &Path,
        herdr_session: &str,
        environment: &[String],
    ) -> String;
}

impl PreparesClaudeEnvironment for str {
    fn claude_environment_preparation(
        &self,
        reserved: Option<&str>,
        socket: &Path,
        herdr: &Path,
        herdr_session: &str,
        environment: &[String],
    ) -> String {
        let marker_suffix = self;
        let flow_id = reserved
            .map(|flow_id| format!(" && export FLOW_ID={flow_id}"))
            .unwrap_or_default();
        let leave_outs = environment
            .iter()
            .map(|variable| format!(" && export {}", variable.shell_quoted()))
            .collect::<String>();
        format!(
            "unset {} && export FLOW_SOCKET={} && export FLOW_HERDR_EXECUTABLE={} && export FLOW_HERDR_SESSION={}{flow_id}{leave_outs} && printf 'FLOW_CLAUDE_ENV_READY_%s\\n' {}",
            HerdrCli::CLAUDE_INHERITED_ENVIRONMENT.join(" "),
            socket.to_string_lossy().shell_quoted(),
            herdr.to_string_lossy().shell_quoted(),
            herdr_session.shell_quoted(),
            marker_suffix
        )
    }
}

/// One shell word holding exactly this text: single-quoted, each single
/// quote closed, escaped and reopened.
pub(crate) trait QuotesForShell {
    fn shell_quoted(&self) -> String;
}

impl QuotesForShell for str {
    fn shell_quoted(&self) -> String {
        format!("'{}'", self.replace('\'', "'\\''"))
    }
}

pub(crate) trait NamesHerdrHarness {
    fn expected_harness(&self) -> &'static str;
}

impl NamesHerdrHarness for HarnessKind {
    fn expected_harness(&self) -> &'static str {
        let harness = self;
        match harness {
            HarnessKind::Claude => "claude",
            HarnessKind::Codex => "codex",
        }
    }
}

pub(crate) trait ChecksComposedLaunch {
    fn launch_agent_name(&self) -> String;
    /// The SHA-256 of every part of the first USER message, in order.
    fn part_hashes(&self) -> Vec<String>;
    /// The launch's harness and its effective leave-outs.
    fn lean_start(&self) -> LeanStart;
    fn binding_matches_launch(&self, pane: &HerdrPaneBinding) -> Result<(), String>;
    fn prompt_intent_matches_launch(&self, intent: &PromptDeliveryIntent) -> Result<(), String>;
}

impl ChecksComposedLaunch for ComposedLaunch {
    fn launch_agent_name(&self) -> String {
        let launch = self;
        let hash = format!(
            "{:x}",
            Sha256::digest(
                format!(
                    "flow-herdr-agent-v1\0{}",
                    launch.launch_profile.launch_request_id
                )
                .as_bytes()
            )
        );
        format!("{}-{}", launch.harness_kind.expected_harness(), &hash[..24])
    }

    fn lean_start(&self) -> LeanStart {
        LeanStart {
            harness_kind: self.harness_kind.clone(),
            leave_outs: self.leave_outs.clone(),
            tool_set: self.launch_profile.tool_set.clone(),
        }
    }

    fn part_hashes(&self) -> Vec<String> {
        self.first_prompt_payload
            .prompt_part_vector
            .iter()
            .map(|part| part.prompt_sha256.clone())
            .collect()
    }

    fn binding_matches_launch(&self, pane: &HerdrPaneBinding) -> Result<(), String> {
        let launch = self;
        if pane.launch_request_id != launch.launch_profile.launch_request_id
            || pane.herdr_session_name != launch.launch_profile.herdr_session_name
            || pane.herdr_agent_name != launch.launch_agent_name()
        {
            return Err("Herdr pane binding does not belong to this launch request".into());
        }
        Ok(())
    }

    fn prompt_intent_matches_launch(&self, intent: &PromptDeliveryIntent) -> Result<(), String> {
        let launch = self;
        launch.binding_matches_launch(&intent.herdr_pane_binding)?;
        if intent.launch_request_id != launch.launch_profile.launch_request_id
            || intent.prompt_sha256 != launch.first_prompt_payload.prompt_sha256
            || intent.harness_kind != launch.harness_kind
            || intent.model_name != launch.launch_profile.model_name
            || intent.effort != launch.launch_profile.effort
            || intent.prompt_sha256_vector != launch.part_hashes()
            || intent.leave_outs != launch.leave_outs
            || !usize::try_from(intent.parts_submitted).is_ok_and(|parts| {
                (intent.prompt_sha256_vector.is_empty() && parts == 0)
                    || (1..=intent.prompt_sha256_vector.len()).contains(&parts)
            })
            || !intent.boundary_matches_intent()
        {
            return Err("durable prompt intent does not belong to this composed launch".into());
        }
        Ok(())
    }
}

pub(crate) trait ReadsLaunchFiles {
    fn collect_native_transcripts(
        &self,
        native_session_id: &str,
        harness: &HarnessKind,
        depth: usize,
        found: &mut Vec<PathBuf>,
    ) -> Result<(), String>;
    fn one_resolved_transcript(&self, found: Vec<PathBuf>) -> Result<PathBuf, String>;
}

impl ReadsLaunchFiles for Path {
    fn collect_native_transcripts(
        &self,
        native_session_id: &str,
        harness: &HarnessKind,
        depth: usize,
        found: &mut Vec<PathBuf>,
    ) -> Result<(), String> {
        let directory = self;
        if depth > 8 || found.len() > 1 {
            return Ok(());
        }
        let entries = fs::read_dir(directory)
            .map_err(|error| format!("native transcript directory is unreadable: {error}"))?;
        for entry in entries {
            let entry = entry
                .map_err(|error| format!("native transcript directory entry failed: {error}"))?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)
                .map_err(|error| format!("native transcript metadata failed: {error}"))?;
            if metadata.file_type().is_symlink() {
                continue;
            }
            if metadata.is_dir() {
                path.collect_native_transcripts(native_session_id, harness, depth + 1, found)?;
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let matches = match harness {
                HarnessKind::Codex => name.ends_with(".jsonl") && name.contains(native_session_id),
                HarnessKind::Claude => name == format!("{native_session_id}.jsonl"),
            };
            if metadata.is_file() && matches {
                found.push(path);
            }
        }
        Ok(())
    }

    fn one_resolved_transcript(&self, found: Vec<PathBuf>) -> Result<PathBuf, String> {
        let root = self;
        if found.len() != 1 {
            return Err("native transcript identity did not resolve to exactly one file".into());
        }
        let resolved = found
            .into_iter()
            .next()
            .expect("one native transcript")
            .canonicalize()
            .map_err(|error| format!("native transcript could not be resolved: {error}"))?;
        resolved
            .starts_with(root)
            .then_some(resolved)
            .ok_or_else(|| "native transcript resolved outside its configured root".into())
    }
}

pub(crate) trait ChecksDeliveryIntent {
    fn boundary_matches_intent(&self) -> bool;
}

impl ChecksDeliveryIntent for PromptDeliveryIntent {
    fn boundary_matches_intent(&self) -> bool {
        let intent = self;
        let (native_session_id, harness_kind) = match &intent.native_transcript_boundary {
            NativeTranscriptBoundary::Existing(cursor) => {
                (&cursor.native_session_id, &cursor.harness_kind)
            }
            NativeTranscriptBoundary::Absent(absence) => {
                (&absence.native_session_id, &absence.harness_kind)
            }
        };
        native_session_id == &intent.native_session_id && harness_kind == &intent.harness_kind
    }
}

pub(crate) trait HashesPrefix {
    fn hash_prefix(&mut self, length: u64) -> Result<(String, Option<u8>), String>;
}

impl HashesPrefix for File {
    fn hash_prefix(&mut self, length: u64) -> Result<(String, Option<u8>), String> {
        let file = self;
        file.seek(SeekFrom::Start(0))
            .map_err(|error| format!("native transcript seek failed: {error}"))?;
        let mut remaining = length;
        let mut hasher = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        let mut last_byte = None;
        while remaining > 0 {
            let wanted =
                usize::try_from(remaining.min(buffer.len() as u64)).expect("bounded buffer length");
            let read = file
                .read(&mut buffer[..wanted])
                .map_err(|error| format!("native transcript prefix read failed: {error}"))?;
            if read == 0 {
                return Err("native transcript was truncated during prefix observation".into());
            }
            hasher.update(&buffer[..read]);
            last_byte = Some(buffer[read - 1]);
            remaining -= read as u64;
        }
        Ok((format!("{:x}", hasher.finalize()), last_byte))
    }
}

pub(crate) trait ReadsLaunchText {
    fn decimal_identity(&self, label: &str) -> Result<u64, String>;
    /// The SHA-256 of the exact text, as a part's hash names it.
    fn text_sha256(&self) -> String;
    /// Claude Code 2.1.294 records a pasted part as one element (`sOe`):
    /// two newlines, `<pasted_content id="ID">` and a newline, the part
    /// under its native paste escape, a newline only when the escaped part
    /// does not already end in one, `</pasted_content id="ID">`, and a
    /// final newline. ID is four lowercase hexadecimal digits.
    ///
    /// Returns the part this row carries when the row is that element for
    /// a part whose SHA-256 is `part_sha256`: the element rebuilt from the
    /// part under the row's ID equals the row byte for byte. The part is
    /// sought among the texts the row can have been built from: the body
    /// as it stands and without its final newline, and at each escaped
    /// tag the `<` the escape replaced or a `<\` the part already held.
    /// A part whose tag opened with another character the escape replaces,
    /// or a row with more than eight escaped tags, is not found.
    fn claude_pasted_part(&self, part_sha256: &str) -> Option<String>;
    /// The element Claude Code records for this part pasted under
    /// `identifier`.
    fn claude_paste_element(&self, identifier: &str) -> String;
    /// This text under Claude Code's paste escape (`qfe`): each opening
    /// character that begins a `pasted_content` tag, as the escape reads
    /// one, becomes `<\`.
    fn claude_paste_escaped(&self) -> String;
    /// Whether this text, read right after an opening character, begins a
    /// `pasted_content` tag as Claude Code's paste escape reads one.
    fn claude_paste_tag_follows(&self) -> bool;
    /// The text after this text's first character, when that character is
    /// in `class`, a set of inclusive code point ranges in ascending order.
    fn after_first_in(&self, class: &[(u32, u32)]) -> Option<&str>;
}

impl ReadsLaunchText for str {
    fn decimal_identity(&self, label: &str) -> Result<u64, String> {
        let value = self;
        let parsed = value
            .parse::<u64>()
            .map_err(|_| format!("native transcript {label} is invalid"))?;
        if parsed.to_string() != value {
            return Err(format!("native transcript {label} is not canonical"));
        }
        Ok(parsed)
    }

    fn text_sha256(&self) -> String {
        format!("{:x}", Sha256::digest(self.as_bytes()))
    }

    fn claude_pasted_part(&self, part_sha256: &str) -> Option<String> {
        let row = self;
        let rest = row.strip_prefix("\n\n<pasted_content id=\"")?;
        let identifier = rest.get(..4)?;
        if !identifier
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        {
            return None;
        }
        let body = rest[4..]
            .strip_prefix("\">\n")?
            .strip_suffix(&format!("</pasted_content id=\"{identifier}\">\n"))?;
        [Some(body), body.strip_suffix('\n')]
            .into_iter()
            .flatten()
            .flat_map(|escaped| {
                let tags = escaped
                    .match_indices("<\\")
                    .map(|(at, _)| at)
                    .filter(|at| escaped[at + 2..].claude_paste_tag_follows())
                    .collect::<Vec<_>>();
                let choices = if tags.len() > 8 {
                    0
                } else {
                    1_u32 << tags.len()
                };
                (0..choices).map(move |choice| {
                    let mut part = String::with_capacity(escaped.len());
                    let mut from = 0;
                    for (bit, at) in tags.iter().enumerate() {
                        if choice & (1 << bit) != 0 {
                            part.push_str(&escaped[from..=*at]);
                            from = at + 2;
                        }
                    }
                    part.push_str(&escaped[from..]);
                    part
                })
            })
            .find(|part| {
                part.text_sha256() == *part_sha256 && part.claude_paste_element(identifier) == *row
            })
    }

    fn claude_paste_element(&self, identifier: &str) -> String {
        let escaped = self.claude_paste_escaped();
        let newline = if escaped.ends_with('\n') { "" } else { "\n" };
        format!(
            "\n\n<pasted_content id=\"{identifier}\">\n{escaped}{newline}</pasted_content id=\"{identifier}\">\n"
        )
    }

    fn claude_paste_escaped(&self) -> String {
        let text = self;
        let mut escaped = String::with_capacity(text.len());
        for (at, character) in text.char_indices() {
            let opens_tag = text[at..]
                .after_first_in(claude_paste::OPENING)
                .is_some_and(|after| !after.starts_with('\\') && after.claude_paste_tag_follows());
            if opens_tag {
                escaped.push_str("<\\");
            } else {
                escaped.push(character);
            }
        }
        escaped
    }

    fn claude_paste_tag_follows(&self) -> bool {
        let mut text = self;
        while let Some(after) = text.after_first_in(claude_paste::FILLER) {
            text = after;
        }
        for (index, letter) in claude_paste::TAG_NAME.iter().enumerate() {
            if index > 0 {
                while let Some(after) = text.after_first_in(claude_paste::GAP) {
                    text = after;
                }
            }
            match text.after_first_in(letter) {
                Some(after) => text = after,
                None => return false,
            }
        }
        text.is_empty() || text.after_first_in(claude_paste::TAIL).is_some()
    }

    fn after_first_in(&self, class: &[(u32, u32)]) -> Option<&str> {
        let mut characters = self.chars();
        let first = u32::from(characters.next()?);
        class
            .binary_search_by(|&(low, high)| {
                if high < first {
                    std::cmp::Ordering::Less
                } else if low > first {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .ok()?;
        Some(characters.as_str())
    }
}

pub(crate) trait ReadsTranscriptRow {
    /// The uuid of a Claude assistant row of this native session.
    fn assistant_turn(&self, native_session_id: &str) -> Option<String>;
    /// Whether a Claude assistant row ran at the intended effort. Claude Code
    /// 2.1.280 records `effort` (and `perTurnEffort`) only for a model that
    /// takes one; for Claude Haiku 4.5 it records no `effort` and
    /// `perTurnEffort: null`. A row that names no effort ran at none, which is
    /// no evidence against the intent; a row that names another one is.
    fn claude_effort_matches(&self, effort: &str) -> bool;
}

impl ReadsTranscriptRow for serde_json::Value {
    fn assistant_turn(&self, native_session_id: &str) -> Option<String> {
        let row = self;
        (row.get("type").and_then(serde_json::Value::as_str) == Some("assistant")
            && row
                .get("sessionId")
                .or_else(|| row.get("session_id"))
                .and_then(serde_json::Value::as_str)
                == Some(native_session_id))
        .then(|| row.get("uuid").and_then(serde_json::Value::as_str))
        .flatten()
        .map(str::to_owned)
    }

    fn claude_effort_matches(&self, effort: &str) -> bool {
        let row = self;
        ["effort", "perTurnEffort"]
            .iter()
            .filter_map(|key| row.get(*key))
            .filter(|value| !value.is_null())
            .all(|value| value.as_str() == Some(effort))
    }
}

pub(crate) trait TitlesClaudeSession {
    fn title_readback_attempts(&self) -> usize;
    fn title_readback_interval(&self) -> Duration;
    /// The source under which Flow reports a pane's displayed agent to Herdr.
    const DISPLAY_SOURCE: &'static str = "flow";
    /// Every title the native Claude transcript records for its session, in
    /// the order written; empty while no transcript or no title record
    /// exists.
    fn claude_transcript_titles(
        &self,
        pane: &HerdrPaneBinding,
        native_session_id: &str,
    ) -> Result<Vec<String>, String>;
    /// Waits, through Herdr's `agent wait`, for a just-bound Claude to come
    /// to rest, then for Herdr to report its official native session and
    /// interactive readiness; returns the agent Herdr then reports.
    fn await_claude_title_readiness(
        &self,
        binding: &NativeLaunchBinding,
    ) -> Result<serde_json::Value, TitleRefusal>;
    /// Reads back the title the Claude session was named with at launch
    /// (`--name`): the terminal title Herdr reports must be the title, and
    /// the session's latest transcript title record, once one is written,
    /// must be the title too. Nothing is typed.
    fn title_claude_session(
        &self,
        binding: &NativeLaunchBinding,
        title: &NativeTitle,
    ) -> Result<(), TitleRefusal>;
    /// Labels the launch pane with the title, shows the title as the pane's
    /// agent in place of the internal Herdr agent name, and reads both back.
    fn label_herdr_pane(&self, pane: &HerdrPaneBinding, title: &NativeTitle) -> Result<(), String>;
    /// Names the launch workspace and the pane's displayed agent with the
    /// title before any agent runs in the pane. Nothing is read back.
    fn show_title_before_agent(
        &self,
        pane: &HerdrPaneBinding,
        title: &NativeTitle,
    ) -> Result<(), String>;
    /// Labels the launch workspace with the title and reads the label back.
    fn label_herdr_workspace(
        &self,
        pane: &HerdrPaneBinding,
        title: &NativeTitle,
    ) -> Result<(), String>;
}

impl TitlesClaudeSession for HerdrCli {
    fn title_readback_attempts(&self) -> usize {
        self.limits.title_readbacks as usize
    }

    fn title_readback_interval(&self) -> Duration {
        Duration::from_millis(self.timings.title_readback_interval as u64)
    }
    fn claude_transcript_titles(
        &self,
        pane: &HerdrPaneBinding,
        native_session_id: &str,
    ) -> Result<Vec<String>, String> {
        let (root, _, found) =
            self.transcript_candidates(pane, native_session_id, &HarnessKind::Claude, None)?;
        if found.is_empty() {
            return Ok(Vec::new());
        }
        let transcript = fs::read(root.one_resolved_transcript(found)?)
            .map_err(|error| format!("native transcript is unreadable: {error}"))?;
        Ok(transcript
            .split(|byte| *byte == b'\n')
            .filter_map(|line| serde_json::from_slice::<serde_json::Value>(line).ok())
            .filter(|row| {
                row.get("type").and_then(serde_json::Value::as_str) == Some("custom-title")
                    && row.get("sessionId").and_then(serde_json::Value::as_str)
                        == Some(native_session_id)
            })
            .filter_map(|row| {
                row.get("customTitle")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            })
            .collect())
    }

    fn await_claude_title_readiness(
        &self,
        binding: &NativeLaunchBinding,
    ) -> Result<serde_json::Value, TitleRefusal> {
        let pane = &binding.herdr_pane_binding;
        // Flow-owned Claude is not a Herdr-managed agent, so Herdr's
        // `agent wait` cannot be its readiness authority.  The target check
        // below joins Herdr's observed idle pane with the selected session in
        // the actual foreground argv.
        let mut last = TitleRefusal::NotReady;
        for _ in 0..self.title_readback_attempts() {
            match self.verify_native_target(pane, &binding.native_session_id, &HarnessKind::Claude)
            {
                Ok(agent)
                    if agent
                        .get("agent_status")
                        .and_then(serde_json::Value::as_str)
                        == Some("idle") =>
                {
                    return Ok(agent);
                }
                Ok(_) => last = TitleRefusal::NotReady,
                Err(NativeTargetRefusal::NoNativeSession) => {
                    last = TitleRefusal::Target(NativeTargetRefusal::NoNativeSession);
                }
                Err(refusal) => return Err(refusal.into()),
            }
            std::thread::sleep(self.title_readback_interval());
        }
        Err(last)
    }

    fn title_claude_session(
        &self,
        binding: &NativeLaunchBinding,
        title: &NativeTitle,
    ) -> Result<(), TitleRefusal> {
        let pane = &binding.herdr_pane_binding;
        self.await_claude_title_readiness(binding)?;
        for _ in 0..self.title_readback_attempts() {
            let agent =
                self.verify_native_target(pane, &binding.native_session_id, &HarnessKind::Claude)?;
            let terminal = agent
                .get("terminal_title_stripped")
                .and_then(serde_json::Value::as_str);
            let recorded = self
                .claude_transcript_titles(pane, &binding.native_session_id)
                .map_err(|_| TitleRefusal::TranscriptUnreadable)?;
            let record_agrees = recorded
                .last()
                .is_none_or(|record| record.as_str() == title.as_str());
            if terminal == Some(title.as_str()) && record_agrees {
                return Ok(());
            }
            std::thread::sleep(self.title_readback_interval());
        }
        Err(TitleRefusal::ReadbackDiffers)
    }

    fn label_herdr_pane(&self, pane: &HerdrPaneBinding, title: &NativeTitle) -> Result<(), String> {
        self.run_json(&[
            "--session".into(),
            pane.herdr_session_name.clone(),
            "pane".into(),
            "rename".into(),
            pane.herdr_pane_id.clone(),
            title.as_str().into(),
        ])?;
        // Herdr shows a pane's agent by its reported display agent, else by
        // its agent name; Flow reports the title, unexpiring and unguarded,
        // so the name never reaches the living.
        // Herdr answers report-metadata with no output: run it for its status.
        self.run_status(&[
            "--session".into(),
            pane.herdr_session_name.clone(),
            "pane".into(),
            "report-metadata".into(),
            pane.herdr_pane_id.clone(),
            "--source".into(),
            Self::DISPLAY_SOURCE.into(),
            "--display-agent".into(),
            title.as_str().into(),
        ])?;
        let response = self.run_json(&[
            "--session".into(),
            pane.herdr_session_name.clone(),
            "pane".into(),
            "get".into(),
            pane.herdr_pane_id.clone(),
        ])?;
        let read = response
            .pointer("/result/pane")
            .ok_or_else(|| "Herdr pane get returned no pane".to_owned())?;
        if read.get("pane_id").and_then(serde_json::Value::as_str)
            != Some(pane.herdr_pane_id.as_str())
            || read.get("terminal_id").and_then(serde_json::Value::as_str)
                != Some(pane.herdr_terminal_id.as_str())
            || read.get("label").and_then(serde_json::Value::as_str) != Some(title.as_str())
            || read
                .get("display_agent")
                .and_then(serde_json::Value::as_str)
                != Some(title.as_str())
        {
            return Err("Herdr pane label readback differs from the title".into());
        }
        Ok(())
    }

    fn show_title_before_agent(
        &self,
        pane: &HerdrPaneBinding,
        title: &NativeTitle,
    ) -> Result<(), String> {
        self.run_json(&[
            "--session".into(),
            pane.herdr_session_name.clone(),
            "workspace".into(),
            "rename".into(),
            pane.herdr_workspace_id.clone(),
            title.as_str().into(),
        ])?;
        // Herdr answers report-metadata with no output: run it for its status.
        self.run_status(&[
            "--session".into(),
            pane.herdr_session_name.clone(),
            "pane".into(),
            "report-metadata".into(),
            pane.herdr_pane_id.clone(),
            "--source".into(),
            Self::DISPLAY_SOURCE.into(),
            "--display-agent".into(),
            title.as_str().into(),
        ])?;
        Ok(())
    }

    fn label_herdr_workspace(
        &self,
        pane: &HerdrPaneBinding,
        title: &NativeTitle,
    ) -> Result<(), String> {
        self.run_json(&[
            "--session".into(),
            pane.herdr_session_name.clone(),
            "workspace".into(),
            "rename".into(),
            pane.herdr_workspace_id.clone(),
            title.as_str().into(),
        ])?;
        let response = self.run_json(&[
            "--session".into(),
            pane.herdr_session_name.clone(),
            "workspace".into(),
            "get".into(),
            pane.herdr_workspace_id.clone(),
        ])?;
        let read = response
            .pointer("/result/workspace")
            .ok_or_else(|| "Herdr workspace get returned no workspace".to_owned())?;
        if read.get("workspace_id").and_then(serde_json::Value::as_str)
            != Some(pane.herdr_workspace_id.as_str())
            || read.get("label").and_then(serde_json::Value::as_str) != Some(title.as_str())
        {
            return Err("Herdr workspace label readback differs from the title".into());
        }
        Ok(())
    }
}

impl TitlesNativeFlow for HerdrCli {
    fn title_native_flow(
        &self,
        launch: &ComposedLaunch,
        binding: &NativeLaunchBinding,
    ) -> Result<NativeTitle, TitleRefusal> {
        if launch
            .binding_matches_launch(&binding.herdr_pane_binding)
            .is_err()
            || binding.launch_request_id != launch.launch_profile.launch_request_id
            || binding.harness_kind != launch.harness_kind
        {
            return Err(TitleRefusal::ForeignBinding);
        }
        let title = launch.launch_profile.native_title(&binding.flow_id)?;
        match binding.harness_kind {
            HarnessKind::Claude => self.title_claude_session(binding, &title)?,
            HarnessKind::Codex => self
                .codex_endpoints
                .adapter_for(&launch.launch_profile.model_name)
                .and_then(|adapter| {
                    adapter.name_bound_thread(&binding.native_session_id, title.as_str())
                })
                .map_err(|_| TitleRefusal::CodexNamingRefused)?,
        }
        self.label_herdr_pane(&binding.herdr_pane_binding, &title)
            .map_err(|_| TitleRefusal::PaneLabelRefused)?;
        self.label_herdr_workspace(&binding.herdr_pane_binding, &title)
            .map_err(|_| TitleRefusal::WorkspaceLabelRefused)?;
        Ok(title)
    }
}

impl CreatesHerdrLaunchPane for HerdrCli {
    fn create_launch_pane(&self, launch: &ComposedLaunch) -> Result<HerdrPaneBinding, String> {
        let agent_name = launch.launch_agent_name();
        let response = self.run_json(&[
            "--session".into(),
            launch.launch_profile.herdr_session_name.clone(),
            "workspace".into(),
            "create".into(),
            "--cwd".into(),
            self.configured_workspace_root()?
                .to_str()
                .ok_or_else(|| "configured workspace root is not UTF-8".to_owned())?
                .into(),
            // No label: the agent name is internal, and the workspace is
            // labelled with the title once the Flow ID is claimed.
            "--env".into(),
            format!(
                "FLOW_LAUNCH_REQUEST_ID={}",
                launch.launch_profile.launch_request_id
            ),
            "--no-focus".into(),
        ])?;
        let workspace = response
            .pointer("/result/workspace/workspace_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "Herdr workspace create returned no workspace id".to_owned())?;
        let pane = response
            .pointer("/result/root_pane/pane_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "Herdr workspace create returned no root pane id".to_owned())?;
        let terminal = response
            .pointer("/result/root_pane/terminal_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| "Herdr workspace create returned no terminal id".to_owned())?;
        Ok(HerdrPaneBinding {
            launch_request_id: launch.launch_profile.launch_request_id.clone(),
            herdr_session_name: launch.launch_profile.herdr_session_name.clone(),
            herdr_agent_name: agent_name,
            herdr_workspace_id: workspace.into(),
            herdr_pane_id: pane.into(),
            herdr_terminal_id: terminal.into(),
        })
    }
}

impl StartsNativeHerdrHarness for HerdrCli {
    fn start_native_harness(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
        reserved: Option<&str>,
    ) -> Result<(), SpawnFailure> {
        // A reserved FlowId was claimed for the session Flow chose; the
        // harness runs as that session.
        let session = match reserved.and_then(|_| self.reserved_native_session_id(launch)) {
            Some(session) => ClaudeSession::Chosen(session),
            None => ClaudeSession::Unchosen,
        };
        self.spawn_native_harness(launch, pane, reserved, session)
    }

    fn attach_codex_harness(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
        thread: &str,
    ) -> Result<(), SpawnFailure> {
        if launch.harness_kind != HarnessKind::Codex {
            return Err("only a Codex harness attaches to an opened thread".into());
        }
        if thread.is_empty() {
            return Err("an attached Codex harness needs its opened thread".into());
        }
        self.spawn_native_harness(launch, pane, None, ClaudeSession::Attached(thread.into()))
    }
}

impl SpawnsNativeHarness for HerdrCli {
    fn spawn_native_harness(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
        reserved: Option<&str>,
        session: ClaudeSession,
    ) -> Result<(), SpawnFailure> {
        launch.binding_matches_launch(pane)?;
        // A Claude session is named at start with its title, so its Flow ID
        // is known before the harness exists: Reserve claimed it for the
        // session Flow chose. The title is the one name the living sees,
        // given as the session name and the Remote Control name alike.
        let claude_title = match (&launch.harness_kind, reserved) {
            (HarnessKind::Claude, Some(flow_id)) => Some(
                launch
                    .launch_profile
                    .native_title(flow_id)
                    .map_err(|refusal| SpawnFailure::Unattempted(refusal.to_string()))?,
            ),
            (HarnessKind::Claude, None) => {
                return Err(SpawnFailure::Unattempted(
                    "a Claude start needs its reserved Flow ID to be named by its title".into(),
                ));
            }
            (HarnessKind::Codex, _) => None,
        };
        // The title is known, so the living sees it on the workspace and as
        // the pane's displayed agent before any agent runs in the pane;
        // Herdr never shows the internal agent name. The title step reads
        // every label back and refuses on a difference, so a Herdr refusal
        // here is logged, not final.
        if let Some(title) = &claude_title
            && let Err(error) = self.show_title_before_agent(pane, title)
        {
            eprintln!("flow-nexus: title not shown before the agent ran: {error}");
        }
        if launch.harness_kind == HarnessKind::Claude {
            self.prepare_claude_pane_environment(launch, pane, reserved)?;
        }
        let harness = launch.harness_kind.expected_harness();
        let mut arguments = vec![
            "--session".into(),
            pane.herdr_session_name.clone(),
            "agent".into(),
            "start".into(),
            pane.herdr_agent_name.clone(),
            "--kind".into(),
            harness.into(),
            "--pane".into(),
            pane.herdr_pane_id.clone(),
            "--timeout".into(),
            self.timings.agent_start_timeout.to_string(),
        ];
        // Herdr starts the exact executable selected by the configured
        // harness installation, for Claude and Codex alike.
        let installation = self.codex_endpoints.harnesses.iter().find(|installation| {
            installation.harness_kind == launch.harness_kind
                && installation
                    .served_models
                    .iter()
                    .any(|model| model.model_name == launch.launch_profile.model_name)
        });
        let program = match installation {
            Some(installation) => installation.executable.clone(),
            #[cfg(test)]
            None => format!("/fixture/{harness}"),
            #[cfg(not(test))]
            None => {
                return Err("configured harness installation is unavailable"
                    .to_owned()
                    .into());
            }
        };
        if !Path::new(&program).is_absolute() {
            return Err("configured harness executable must be absolute"
                .to_owned()
                .into());
        }
        arguments.push("--executable".into());
        arguments.push(program.clone());
        let codex_endpoint = if launch.harness_kind == HarnessKind::Codex {
            let endpoint = self
                .codex_endpoints
                .endpoint_for(&launch.launch_profile.model_name)
                .map_err(|error| error.to_string())?;
            if !Path::new(&endpoint.socket).is_absolute() {
                return Err("configured Codex socket must be absolute".to_owned().into());
            }
            Some(endpoint)
        } else {
            None
        };
        arguments.push("--".into());
        let typed_from = arguments.len();
        if let Some(endpoint) = codex_endpoint {
            arguments.push("--remote".into());
            arguments.push(format!("unix://{}", endpoint.socket));
        }
        if let Some(title) = &claude_title {
            arguments.push("--settings".into());
            arguments.push(self.claude_flag_settings(&launch.lean_start()));
            // Every Claude Flow is remotely controllable under its title;
            // the title begins with `{`, never `-`, so the optional value
            // binds to the flag.
            arguments.push(Self::CLAUDE_REMOTE_CONTROL_FLAG.into());
            arguments.push(title.as_str().into());
            // The session is named at launch with the same title: the name
            // Claude records in its transcript and shows in the prompt box,
            // the resume picker and the terminal title. `/rename` is a
            // slash command, which a launch leaving out the Skill tool
            // (`--disable-slash-commands`) cannot run.
            arguments.push(Self::CLAUDE_NAME_FLAG.into());
            arguments.push(title.as_str().into());
            // The composed system prompt is read from the in-memory file
            // this Nexus holds open until the session has bound; it never
            // travels in the typed command and never touches a disk.
            arguments.push("--system-prompt-file".into());
            arguments.push(self.hold_system_prompt(launch)?);
            match &session {
                ClaudeSession::Unchosen | ClaudeSession::Attached(_) => {}
                ClaudeSession::Chosen(session) => {
                    arguments.push("--session-id".into());
                    arguments.push(session.clone());
                }
            }
        }
        arguments.push("--model".into());
        arguments.push(launch.launch_profile.model_name.clone());
        match launch.harness_kind {
            HarnessKind::Claude => {
                arguments.push("--effort".into());
                arguments.push(launch.launch_profile.effort.clone());
            }
            // The thread already holds the composed system prompt as its
            // base instructions, given at thread/start and given again after
            // the TUI attaches; the TUI is not given it.
            HarnessKind::Codex => {
                arguments.push("-c".into());
                arguments.push(format!(
                    "model_reasoning_effort={}",
                    launch.launch_profile.effort
                ));
                // The terminal title is the thread title alone, which Title
                // sets to the seat's struct: codex-tui 0.161.0-alpha.2 names
                // its items in `[tui].terminal_title`, and without this list
                // appends the project name (" | primary").
                arguments.push("-c".into());
                arguments.push(CODEX_TERMINAL_TITLE.into());
            }
        }
        // The launch's leave-outs, as its harness's own switches.
        arguments.extend(launch.lean_start().arguments());
        // The subcommand follows every switch: the TUI resumes the thread
        // Flow opened and named, idle until Submit gives it its first turn.
        if let (HarnessKind::Codex, ClaudeSession::Attached(thread)) =
            (&launch.harness_kind, &session)
        {
            arguments.push("resume".into());
            arguments.push(thread.clone());
        }
        let typed = std::iter::once(program)
            .chain(arguments[typed_from..].iter().cloned())
            .collect::<Vec<_>>();
        if let Some(cause) = typed.typed_command_refusal() {
            self.release_system_prompt(&launch.launch_profile.launch_request_id);
            return Err(SpawnFailure::Refused(cause));
        }
        if launch.harness_kind == HarnessKind::Claude {
            return self.run_flow_owned_claude(pane, &typed).inspect_err(|_| {
                self.release_system_prompt(&launch.launch_profile.launch_request_id)
            });
        }
        // No positional prompt: the first USER message is typed after.
        self.run_agent_start(&arguments)
            .inspect_err(|_| self.release_system_prompt(&launch.launch_profile.launch_request_id))
    }
}

/// Starts the exact configured Claude command in the pane Herdr owns. Flow
/// owns this process lifecycle: Herdr receives one safely quoted shell line,
/// while the native hooks report the session and readiness back as Flow's
/// explicit lifecycle authority.
pub(crate) trait RunsFlowOwnedClaude {
    fn run_flow_owned_claude(
        &self,
        pane: &HerdrPaneBinding,
        command: &[String],
    ) -> Result<(), SpawnFailure>;
}

impl RunsFlowOwnedClaude for HerdrCli {
    fn run_flow_owned_claude(
        &self,
        pane: &HerdrPaneBinding,
        command: &[String],
    ) -> Result<(), SpawnFailure> {
        let runtime = &self.flow_owned_claude_readiness_root;
        let readiness_digest = format!("{:x}", Sha256::digest(pane.herdr_pane_id.as_bytes()));
        let readiness_name = format!("claude-ready-{}.sock", &readiness_digest[..12]);
        let readiness_path = runtime.join(readiness_name);
        let _ = fs::remove_file(&readiness_path);
        let readiness =
            std::os::unix::net::UnixDatagram::bind(&readiness_path).map_err(|error| {
                SpawnFailure::Unattempted(format!(
                    "Flow-owned Claude readiness endpoint could not open: {error}"
                ))
            })?;
        self.flow_owned_claude_readiness
            .lock()
            .map_err(|_| {
                SpawnFailure::Unattempted(
                    "Flow-owned Claude readiness ownership is unavailable".into(),
                )
            })?
            .insert(
                pane.herdr_pane_id.clone(),
                (readiness, readiness_path.clone()),
            );
        let readiness_environment = format!(
            "export FLOW_CLAUDE_READY_SOCKET={}",
            readiness_path.to_string_lossy().shell_quoted()
        );
        let readiness_arguments = [
            "--session".into(),
            pane.herdr_session_name.clone(),
            "pane".into(),
            "run".into(),
            pane.herdr_pane_id.clone(),
            readiness_environment,
        ];
        let readiness_output = Command::new(&self.executable)
            .args(&readiness_arguments)
            .output()
            .map_err(|error| {
                SpawnFailure::Unattempted(format!("Herdr command could not start: {error}"))
            })?;
        if !readiness_output.status.success() {
            if let Ok(mut readiness) = self.flow_owned_claude_readiness.lock() {
                readiness.remove(&pane.herdr_pane_id);
            }
            let _ = fs::remove_file(readiness_path);
            return Err(SpawnFailure::Refused(
                signal_flow::SpawnRefusalCause::Herdr(signal_flow::HerdrRefusal::from_printed(
                    &readiness_output.stderr,
                    &readiness_output.stdout,
                )),
            ));
        }
        let line = command
            .iter()
            .map(|argument| argument.as_str().shell_quoted())
            .collect::<Vec<_>>()
            .join(" ");
        let arguments = [
            "--session".into(),
            pane.herdr_session_name.clone(),
            "pane".into(),
            "run".into(),
            pane.herdr_pane_id.clone(),
            line,
        ];
        let output = Command::new(&self.executable)
            .args(&arguments)
            .output()
            .map_err(|error| format!("Herdr command could not start: {error}"))?;
        if !output.status.success() {
            if let Ok(mut readiness) = self.flow_owned_claude_readiness.lock() {
                readiness.remove(&pane.herdr_pane_id);
            }
            let _ = fs::remove_file(readiness_path);
            return Err(SpawnFailure::Refused(
                signal_flow::SpawnRefusalCause::Herdr(signal_flow::HerdrRefusal::from_printed(
                    &output.stderr,
                    &output.stdout,
                )),
            ));
        }
        // `herdr pane run` is a status-only command: unlike the query
        // commands, success is deliberately silent.  Its exit status is the
        // complete acknowledgement that the line and Enter were accepted.
        Ok(())
    }
}

/// Why a harness start in its launch pane did not happen: it was refused,
/// by Herdr or by Flow before Herdr was called, or Flow could not attempt
/// it.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SpawnFailure {
    #[error("the harness start was refused")]
    Refused(signal_flow::SpawnRefusalCause),
    #[error("{0}")]
    Unattempted(String),
}

impl From<String> for SpawnFailure {
    fn from(error: String) -> Self {
        SpawnFailure::Unattempted(error)
    }
}

impl From<&str> for SpawnFailure {
    fn from(error: &str) -> Self {
        SpawnFailure::Unattempted(error.to_owned())
    }
}

impl From<SpawnFailure> for String {
    fn from(failure: SpawnFailure) -> Self {
        match failure {
            SpawnFailure::Refused(cause) => format!("the harness start was refused: {cause:?}"),
            SpawnFailure::Unattempted(error) => error,
        }
    }
}

/// The command Herdr types into the launch pane's shell, the program then
/// each argument, as Herdr quotes them for a POSIX shell.
pub(crate) trait ChecksTypedCommand {
    /// The longest command typed as one terminal line: Linux's canonical
    /// line buffer holds 4096 bytes, the last being the line's end. A
    /// longer line is not read whole by a shell that reads it before
    /// leaving canonical mode, and a Codex command of about 16 KiB typed
    /// into a ready bash pane was witnessed cut short.
    const TYPED_COMMAND_LIMIT: usize = 4095;
    /// Why the command must not be typed: an argument carries a control
    /// character, which Herdr 0.8.2 refuses and a shell line cannot hold
    /// whole, or the quoted line is longer than one terminal line.
    fn typed_command_refusal(&self) -> Option<signal_flow::SpawnRefusalCause>;
    fn quoted_length(argument: &str) -> usize;
}

impl ChecksTypedCommand for [String] {
    fn typed_command_refusal(&self) -> Option<signal_flow::SpawnRefusalCause> {
        if self
            .iter()
            .any(|argument| argument.chars().any(char::is_control))
        {
            return Some(signal_flow::SpawnRefusalCause::ControlCharacter);
        }
        let length = self
            .iter()
            .map(|argument| Self::quoted_length(argument))
            .sum::<usize>()
            + self.len().saturating_sub(1);
        (length > Self::TYPED_COMMAND_LIMIT)
            .then_some(signal_flow::SpawnRefusalCause::CommandTooLong)
    }

    /// Herdr's POSIX quoting: a word of safe characters stands bare; any
    /// other is single-quoted, each `'` written `'\''`.
    fn quoted_length(argument: &str) -> usize {
        let bare = !argument.is_empty()
            && argument.chars().all(|character| {
                character.is_ascii_alphanumeric()
                    || matches!(
                        character,
                        '@' | '%' | '_' | '+' | '=' | ':' | ',' | '.' | '/' | '-'
                    )
            });
        if bare {
            argument.len()
        } else {
            argument.len() + 2 + 3 * argument.matches('\'').count()
        }
    }
}

/// The composed system prompt of a Claude start, held by this Nexus in an
/// in-memory file Claude reads through `/proc/<nexus>/fd/<n>`, from the
/// start until the session has bound.
pub(crate) trait HoldsSystemPrompt {
    /// Writes the launch's composed system prompt into a new in-memory file
    /// held under the launch request, and answers the path Claude reads it
    /// by. A file already held for the request is replaced.
    fn hold_system_prompt(&self, launch: &ComposedLaunch) -> Result<String, String>;
    /// Closes the in-memory file held for the launch request, if any.
    fn release_system_prompt(&self, launch_request_id: &str);
    /// The path a held system prompt is read by, for the request, while
    /// held.
    #[cfg(test)]
    fn held_system_prompt_path(&self, launch_request_id: &str) -> Option<String>;
}

impl HoldsSystemPrompt for HerdrCli {
    fn hold_system_prompt(&self, launch: &ComposedLaunch) -> Result<String, String> {
        use std::os::fd::AsRawFd;
        let descriptor =
            rustix::fs::memfd_create("flow-system-prompt", rustix::fs::MemfdFlags::CLOEXEC)
                .map_err(|error| format!("cannot create the in-memory system prompt: {error}"))?;
        let mut file = File::from(descriptor);
        file.write_all(launch.system_prompt.system_prompt_text.as_bytes())
            .map_err(|error| format!("cannot write the in-memory system prompt: {error}"))?;
        let path = format!("/proc/{}/fd/{}", std::process::id(), file.as_raw_fd());
        self.held_system_prompts
            .lock()
            .map_err(|_| "the held system prompts are unreadable".to_owned())?
            .insert(launch.launch_profile.launch_request_id.clone(), file);
        Ok(path)
    }

    fn release_system_prompt(&self, launch_request_id: &str) {
        if let Ok(mut held) = self.held_system_prompts.lock() {
            held.remove(launch_request_id);
        }
    }

    #[cfg(test)]
    fn held_system_prompt_path(&self, launch_request_id: &str) -> Option<String> {
        use std::os::fd::AsRawFd;
        self.held_system_prompts
            .lock()
            .ok()?
            .get(launch_request_id)
            .map(|file| format!("/proc/{}/fd/{}", std::process::id(), file.as_raw_fd()))
    }
}

/// Herdr's `agent start`, whose refusal is kept as Herdr wrote it.
pub(crate) trait StartsHerdrAgent {
    fn run_agent_start(&self, arguments: &[String]) -> Result<(), SpawnFailure>;
}

impl StartsHerdrAgent for HerdrCli {
    fn run_agent_start(&self, arguments: &[String]) -> Result<(), SpawnFailure> {
        let output = Command::new(&self.executable)
            .args(arguments)
            .output()
            .map_err(|error| format!("Herdr command could not start: {error}"))?;
        if !output.status.success() {
            return Err(SpawnFailure::Refused(
                signal_flow::SpawnRefusalCause::Herdr(signal_flow::HerdrRefusal::from_printed(
                    &output.stderr,
                    &output.stdout,
                )),
            ));
        }
        serde_json::from_slice::<serde_json::Value>(&output.stdout)
            .map(|_| ())
            .map_err(|error| SpawnFailure::from(format!("Herdr returned unreadable JSON: {error}")))
    }
}

/// Herdr's refusal as it printed it: the code and message of its JSON error
/// body, or no code and its whole printed text.
pub(crate) trait ReadsHerdrRefusal {
    fn from_printed(stderr: &[u8], stdout: &[u8]) -> Self;
}

impl ReadsHerdrRefusal for signal_flow::HerdrRefusal {
    fn from_printed(stderr: &[u8], stdout: &[u8]) -> Self {
        let printed = if stderr.iter().all(u8::is_ascii_whitespace) {
            stdout
        } else {
            stderr
        };
        let text = String::from_utf8_lossy(printed).trim().to_owned();
        let body = serde_json::from_str::<serde_json::Value>(&text).ok();
        let error = body.as_ref().and_then(|body| body.get("error"));
        let code = error
            .and_then(|error| error.get("code"))
            .and_then(serde_json::Value::as_str);
        let message = error
            .and_then(|error| error.get("message"))
            .and_then(serde_json::Value::as_str);
        match (code, message) {
            (Some(code), Some(message)) => Self {
                herdr_error_code_option: Some(code.to_owned()),
                herdr_error_message: message.to_owned(),
            },
            _ => Self {
                herdr_error_code_option: None,
                herdr_error_message: text,
            },
        }
    }
}

/// Herdr's own wait for a launched agent's pane to come to rest. The launch
/// path never polls for readiness itself: a bind, and a first prompt after
/// the title, each wait here, bounded, and a timeout is a typed not-ready
/// refusal.
pub(crate) trait WaitsForPaneRest {
    /// The refusal text a bind or a prompt that never found the pane at rest
    /// carries; the failure classifier reads it as not-ready.
    const NOT_READY: &'static str = "Herdr agent did not come to rest: not ready";
    fn await_pane_rest(&self, pane: &HerdrPaneBinding) -> Result<(), String>;
}

impl WaitsForPaneRest for HerdrCli {
    fn await_pane_rest(&self, pane: &HerdrPaneBinding) -> Result<(), String> {
        for _ in 0..self.title_readback_attempts() {
            if self
                .run_status(&[
                    "--session".into(),
                    pane.herdr_session_name.clone(),
                    "agent".into(),
                    "wait".into(),
                    pane.herdr_pane_id.clone(),
                    "--until".into(),
                    "idle".into(),
                    "--until".into(),
                    "done".into(),
                    "--timeout".into(),
                    self.timings.launch_rest_wait.to_string(),
                ])
                .is_ok()
            {
                return Ok(());
            }
            std::thread::sleep(self.title_readback_interval());
        }
        Err(Self::NOT_READY.to_owned())
    }
}

impl ObservesNativeLaunchBinding for HerdrCli {
    /// Whatever it observes, the in-memory system prompt held for the
    /// launch is closed after: the started harness has read it by the time
    /// its session can bind.
    fn observe_native_binding(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
        reserved: Option<&str>,
    ) -> Result<NativeLaunchBinding, String> {
        let observed = self.observe_bound_pane(launch, pane, reserved, None);
        self.release_system_prompt(&launch.launch_profile.launch_request_id);
        observed
    }

    /// The Codex thread is the one Flow opened and named through the
    /// app-server: the pane binds to it once Herdr recognizes the
    /// pane's Codex agent, exact and interactively ready. Herdr reports no
    /// agent session for a TUI that remote-resumed a thread, and the
    /// app-server names no signal that a client is attached to a thread
    /// (`thread/loaded/list` names the threads loaded in memory, which the
    /// opening already loaded); the launch receipt then reads the first turn
    /// Submit gives that thread, and its answer. A session Herdr does report must be that
    /// thread.
    fn observe_attached_codex_binding(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
        thread: &str,
    ) -> Result<NativeLaunchBinding, String> {
        if launch.harness_kind != HarnessKind::Codex {
            return Err("only a Codex harness attaches to an opened thread".into());
        }
        if thread.is_empty() {
            return Err("an attached Codex harness needs its opened thread".into());
        }
        let observed = self.observe_bound_pane(launch, pane, None, Some(thread));
        self.release_system_prompt(&launch.launch_profile.launch_request_id);
        observed
    }
}

/// Observes the official Herdr integration report for a launch pane and
/// binds it to an exact native identity claim. `attached` is the Codex
/// thread Flow opened, which the pane's TUI resumed.
pub(crate) trait ObservesBoundPane {
    fn observe_bound_pane(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
        reserved: Option<&str>,
        attached: Option<&str>,
    ) -> Result<NativeLaunchBinding, String>;
}

impl ObservesBoundPane for HerdrCli {
    fn observe_bound_pane(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
        reserved: Option<&str>,
        attached: Option<&str>,
    ) -> Result<NativeLaunchBinding, String> {
        launch.binding_matches_launch(pane)?;
        let expected_harness = launch.harness_kind.expected_harness();
        let flow_owned_claude = launch.harness_kind == HarnessKind::Claude && reserved.is_some();
        // Managed launches use Herdr's lifecycle wait.  A Flow-owned Claude
        // is observed through its exact process and lifecycle hook instead.
        if !flow_owned_claude {
            self.await_pane_rest(pane)?;
        }
        if flow_owned_claude {
            let selected = self
                .reserved_native_session_id(launch)
                .filter(|session| !session.is_empty())
                .ok_or_else(|| {
                    "the Flow-owned Claude process has no selected session".to_owned()
                })?;
            let already_reported = self
                .run_json(&[
                    "--session".into(),
                    pane.herdr_session_name.clone(),
                    "agent".into(),
                    "get".into(),
                    pane.herdr_agent_name.clone(),
                ])
                .ok()
                .and_then(|response| response.pointer("/result/agent/agent_session").cloned())
                .is_some_and(|session| {
                    session.get("source").and_then(serde_json::Value::as_str) == Some("flow")
                        && session.get("agent").and_then(serde_json::Value::as_str)
                            == Some("claude")
                        && session.get("kind").and_then(serde_json::Value::as_str) == Some("id")
                        && session.get("value").and_then(serde_json::Value::as_str)
                            == Some(selected.as_str())
                });
            let (readiness, readiness_path) = self
                .flow_owned_claude_readiness
                .lock()
                .map_err(|_| "Flow-owned Claude readiness ownership is unavailable".to_owned())?
                .remove(&pane.herdr_pane_id)
                .ok_or_else(|| "Flow-owned Claude readiness endpoint is absent".to_owned())?;
            let mut readiness_report = [0_u8; 1];
            let report = if already_reported {
                Ok(0)
            } else {
                readiness
                    .set_read_timeout(Some(Duration::from_millis(
                        self.timings.launch_rest_wait as u64,
                    )))
                    .map_err(|error| {
                        format!("Flow-owned Claude readiness timeout failed: {error}")
                    })?;
                readiness.recv(&mut readiness_report).map_err(|error| {
                    format!("Flow-owned Claude readiness was not reported: {error}")
                })
            };
            let _ = fs::remove_file(readiness_path);
            let report_length = report?;
            if report_length != 0 {
                return Err("Flow-owned Claude readiness endpoint carried unexpected data".into());
            }
            self.verify_flow_owned_claude_process(
                pane,
                &selected,
                &launch.launch_profile.model_name,
            )?;
        }
        let response = self.run_json(&[
            "--session".into(),
            pane.herdr_session_name.clone(),
            "agent".into(),
            "get".into(),
            pane.herdr_agent_name.clone(),
        ])?;
        let agent = response
            .pointer("/result/agent")
            .ok_or_else(|| "Herdr agent get returned no agent record".to_owned())?;
        let exact_name = launch.harness_kind == HarnessKind::Claude
            || agent.get("name").and_then(serde_json::Value::as_str)
                == Some(pane.herdr_agent_name.as_str());
        // A Flow-owned Claude is not a Herdr-managed launch, so Herdr 0.8.2
        // reports its supported observed state (`agent_status`) rather than
        // the managed-launch-only `interactive_ready` field.  The exact
        // process argv above proves the selected session and model; this
        // resting state proves the actual TUI has reached its input boundary.
        let interactive_ready = match launch.harness_kind {
            HarnessKind::Claude => {
                agent
                    .get("agent_status")
                    .and_then(serde_json::Value::as_str)
                    == Some("idle")
            }
            HarnessKind::Codex => {
                agent
                    .get("interactive_ready")
                    .and_then(serde_json::Value::as_bool)
                    == Some(true)
            }
        };
        let exact_pane = exact_name
            && agent.get("agent").and_then(serde_json::Value::as_str) == Some(expected_harness)
            && agent
                .get("workspace_id")
                .and_then(serde_json::Value::as_str)
                == Some(pane.herdr_workspace_id.as_str())
            && agent.get("pane_id").and_then(serde_json::Value::as_str)
                == Some(pane.herdr_pane_id.as_str())
            && agent.get("terminal_id").and_then(serde_json::Value::as_str)
                == Some(pane.herdr_terminal_id.as_str())
            && interactive_ready;
        if !exact_pane {
            return Err("Herdr agent does not match the created native pane".into());
        }
        // Claude receives the Flow-selected session through `--session-id`;
        // a Codex TUI resumes the thread Flow opened. Herdr may not expose
        // `agent_session` before Claude finishes its own initialization, and
        // exposes none for a remote-resumed Codex TUI, so either binds the
        // session Flow selected after exact pane/harness/readiness
        // verification. If Herdr does expose a session, it must agree
        // exactly; it is never silently substituted.
        let selected_session = match &launch.harness_kind {
            HarnessKind::Claude if reserved.is_some() => self
                .reserved_native_session_id(launch)
                .filter(|session| !session.is_empty()),
            HarnessKind::Codex => attached
                .filter(|thread| !thread.is_empty())
                .map(str::to_owned),
            HarnessKind::Claude => None,
        };
        let expected_source = match launch.harness_kind {
            HarnessKind::Claude => "flow".to_owned(),
            HarnessKind::Codex => format!("herdr:{expected_harness}"),
        };
        // The Flow-owned Claude identity comes from the launch-owned
        // readiness handshake and the actual foreground argv, not Herdr's
        // managed-agent session field.  Automatic detection may publish a
        // stale or partial managed value for the same pane.
        let observed_session = (!flow_owned_claude)
            .then(|| agent.get("agent_session"))
            .flatten();
        let observed_native_session = |session: &serde_json::Value| -> Result<String, String> {
            if session.get("source").and_then(serde_json::Value::as_str)
                != Some(expected_source.as_str())
                || session.get("agent").and_then(serde_json::Value::as_str)
                    != Some(expected_harness)
                || session.get("kind").and_then(serde_json::Value::as_str) != Some("id")
            {
                return Err("native identity did not come from the launch lifecycle owner".into());
            }
            session
                .get("value")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| {
                    "official Herdr integration returned an empty native identity".to_owned()
                })
        };
        let native_session_id = if let Some(selected) = selected_session.as_deref() {
            if let Some(session) = observed_session {
                let observed = observed_native_session(session)?;
                if observed != selected {
                    return Err(match launch.harness_kind {
                        HarnessKind::Claude => "observed native identity conflicts with the Flow-selected Claude session",
                        HarnessKind::Codex => "observed native identity conflicts with the Flow-selected session: the attached Codex TUI reports another thread than Flow opened",
                    }
                    .into());
                }
            }
            selected.to_owned()
        } else {
            let session = observed_session.ok_or_else(|| {
                "official Herdr integration reported no native session".to_owned()
            })?;
            observed_native_session(session)?
        };
        let normalized_identity = Self::canonical_native_identity(&native_session_id);
        if reserved.is_some()
            && self.reserved_native_session_id(launch).as_deref()
                != Some(native_session_id.as_str())
        {
            return Err("native session is not the one Flow reserved for this launch".into());
        }
        let flow_id = self.claim_flow_identity(&launch.harness_kind, &native_session_id)?;
        if reserved.is_some_and(|reserved| reserved != flow_id) {
            return Err("flow claim does not name the FlowId Flow reserved".into());
        }
        if self.claimed_native_identity(&flow_id, &launch.harness_kind)? != normalized_identity {
            return Err("flow claim does not contain the observed native identity".into());
        }
        Ok(NativeLaunchBinding {
            launch_request_id: launch.launch_profile.launch_request_id.clone(),
            flow_id,
            native_session_id,
            harness_kind: launch.harness_kind.clone(),
            herdr_pane_binding: pane.clone(),
        })
    }
}

/// Reads the pane's actual foreground process set rather than using agent
/// detection as launch authority. The exact command was authored and typed
/// by Flow; after an installation wrapper execs its child, the selected
/// session and model remain in the actual child's argv and bind it to this
/// launch.
pub(crate) trait VerifiesFlowOwnedClaudeProcess {
    fn verify_flow_owned_claude_process(
        &self,
        pane: &HerdrPaneBinding,
        session: &str,
        model: &str,
    ) -> Result<(), String>;
}

impl VerifiesFlowOwnedClaudeProcess for HerdrCli {
    fn verify_flow_owned_claude_process(
        &self,
        pane: &HerdrPaneBinding,
        session: &str,
        model: &str,
    ) -> Result<(), String> {
        let response = self.run_json(&[
            "--session".into(),
            pane.herdr_session_name.clone(),
            "pane".into(),
            "process-info".into(),
            "--pane".into(),
            pane.herdr_pane_id.clone(),
        ])?;
        let processes = response
            .pointer("/result/process_info/foreground_processes")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "Herdr process-info returned no foreground process set".to_owned())?;
        let matches_pair = |argv: &[serde_json::Value], flag: &str, value: &str| {
            argv.windows(2)
                .any(|pair| pair[0].as_str() == Some(flag) && pair[1].as_str() == Some(value))
        };
        processes
            .iter()
            .filter_map(|process| process.get("argv").and_then(serde_json::Value::as_array))
            .any(|argv| {
                matches_pair(argv, "--session-id", session) && matches_pair(argv, "--model", model)
            })
            .then_some(())
            .ok_or_else(|| {
                "the Flow-owned Claude process does not carry the selected session and model"
                    .to_owned()
            })
    }
}

impl AcceptsLaunchRegistration for HerdrCli {
    fn accept_registration(
        &self,
        launch: &ComposedLaunch,
        binding: &NativeLaunchBinding,
        acknowledgement: &RegistrationAcknowledgement,
    ) -> Result<PromptDeliveryIntent, RegistrationRefusal> {
        launch
            .binding_matches_launch(&binding.herdr_pane_binding)
            .map_err(|_| RegistrationRefusal::PaneBindingForeign)?;
        if binding.harness_kind != launch.harness_kind
            || acknowledgement.launch_request_id != binding.launch_request_id
            || acknowledgement.flow_id != binding.flow_id
            || acknowledgement.native_session_id != binding.native_session_id
            || acknowledgement.herdr_pane_binding != binding.herdr_pane_binding
        {
            return Err(RegistrationRefusal::AcknowledgementMismatch);
        }
        // The target is the thread Flow bound and the recognized pane; the
        // stage is named before the transcript is read.
        self.verify_native_target(
            &binding.herdr_pane_binding,
            &binding.native_session_id,
            &binding.harness_kind,
        )?;
        // A Codex thread's rollout exists from its naming at open, before
        // its TUI attached; its receipt is read from the thread's start.
        let native_transcript_boundary = match self
            .capture_transcript_boundary(binding, &launch.launch_profile.model_name)
            .map_err(RegistrationRefusal::TranscriptBoundary)?
        {
            NativeTranscriptBoundary::Existing(_) if binding.harness_kind == HarnessKind::Codex => {
                self.capture_transcript_start_boundary(binding, &launch.launch_profile.model_name)
                    .map_err(RegistrationRefusal::TranscriptBoundary)?
            }
            boundary => boundary,
        };
        // The transcript is the authority for the thread when Herdr reports
        // none: its first record is the session meta naming exactly the
        // thread Flow opened. The first user turn's part hashes are read
        // against the same transcript by the receipt.
        if binding.harness_kind == HarnessKind::Codex
            && matches!(
                native_transcript_boundary,
                NativeTranscriptBoundary::Existing(_)
            )
        {
            let (root, _, found) = self
                .transcript_candidates(
                    &binding.herdr_pane_binding,
                    &binding.native_session_id,
                    &binding.harness_kind,
                    Some(&launch.launch_profile.model_name),
                )
                .map_err(|refusal| RegistrationRefusal::TranscriptBoundary(refusal.to_string()))?;
            let transcript = root
                .one_resolved_transcript(found)
                .map_err(RegistrationRefusal::TranscriptBoundary)?;
            let mut first = String::new();
            BufReader::new(File::open(transcript).map_err(|error| {
                RegistrationRefusal::TranscriptBoundary(format!(
                    "native transcript is unreadable: {error}"
                ))
            })?)
            .read_line(&mut first)
            .map_err(|error| {
                RegistrationRefusal::TranscriptBoundary(format!(
                    "native transcript read failed: {error}"
                ))
            })?;
            if !serde_json::from_str::<serde_json::Value>(&first)
                .is_ok_and(|row| row.names_thread(&binding.native_session_id))
            {
                return Err(RegistrationRefusal::ThreadNotNamed);
            }
        }
        Ok(PromptDeliveryIntent {
            launch_request_id: binding.launch_request_id.clone(),
            prompt_sha256: launch.first_prompt_payload.prompt_sha256.clone(),
            flow_id: binding.flow_id.clone(),
            native_session_id: binding.native_session_id.clone(),
            herdr_pane_binding: binding.herdr_pane_binding.clone(),
            harness_kind: binding.harness_kind.clone(),
            model_name: launch.launch_profile.model_name.clone(),
            effort: launch.launch_profile.effort.clone(),
            prompt_sha256_vector: launch.part_hashes(),
            // The first part is the one about to be submitted. An empty
            // startup has no native user turn; queued work is its first one.
            parts_submitted: if launch.first_prompt_payload.prompt_part_vector.is_empty() {
                0
            } else {
                1
            },
            stops_reported: 0,
            leave_outs: launch.leave_outs.clone(),
            native_transcript_boundary,
        })
    }
}

impl SubmitsStartupPart for HerdrCli {
    fn submit_startup_part(
        &self,
        launch: &ComposedLaunch,
        durable_intent: &PromptDeliveryIntent,
    ) -> Result<PromptDeliveryResult, String> {
        launch.prompt_intent_matches_launch(durable_intent)?;
        if durable_intent.harness_kind != HarnessKind::Claude {
            return Err("a Codex part is submitted through its app-server".into());
        }
        let index = usize::try_from(durable_intent.parts_submitted - 1)
            .map_err(|_| "no part is counted as submitted".to_owned())?;
        let part = &launch.first_prompt_payload.prompt_part_vector[index];
        // The started harness (for the first part) or the answer to the
        // part before has come to rest; nothing is typed when it has not.
        self.await_pane_rest(&durable_intent.herdr_pane_binding)?;
        self.prompt_through_socket(&durable_intent.herdr_pane_binding, &part.prompt_part_text)?;
        Ok(PromptDeliveryResult::Ambiguous(durable_intent.clone()))
    }
}

/// The text a Claude user row carries as typed input: a string, or a
/// content vector of text items only. A harness notice (isMeta), a tool
/// result or any other item is not typed input.
impl ReadsTypedUserText for serde_json::Value {
    fn claude_typed_text(&self) -> Option<String> {
        if self.get("isMeta").and_then(serde_json::Value::as_bool) == Some(true) {
            return None;
        }
        let content = self.pointer("/message/content")?;
        if let Some(text) = content.as_str() {
            return Some(text.to_owned());
        }
        let items = content.as_array()?;
        if items.is_empty() {
            return None;
        }
        items
            .iter()
            .map(|item| {
                (item.get("type").and_then(serde_json::Value::as_str) == Some("text"))
                    .then(|| item.get("text").and_then(serde_json::Value::as_str))
                    .flatten()
            })
            .collect::<Option<Vec<_>>>()
            .map(|texts| texts.concat())
    }
}

/// One transcript record read against the intent: the receipt turn when
/// the record is the flow's first answer after every part, and a
/// contradiction refuses. A user text is the next part expected, and the
/// count of parts seen grows by it; one that is not refuses while parts
/// are still expected.
impl ReadsStartupRecord for StartupReading {
    fn read_part<N: Textual, O: DeliveryIntended>(
        &mut self,
        input_0: N,
        input_1: O,
    ) -> Result<i64, NativeLaunchFailure> {
        let text = input_0.text();
        let intent = input_1.intent();
        let expected = usize::try_from(self.seen_parts)
            .ok()
            .and_then(|seen| intent.prompt_sha256_vector.get(seen))
            .filter(|_| self.seen_parts < intent.parts_submitted);
        if expected.is_some_and(|hash| &text.text_sha256() == hash) {
            self.seen_parts += 1;
            self.last_part_answered = false;
            return Ok(self.seen_parts);
        }
        Err(NativeLaunchFailure::PartDiffers)
    }

    fn read_record<N: JsonRow, O: DeliveryIntended>(
        &mut self,
        input_0: N,
        input_1: O,
    ) -> Result<Option<String>, NativeLaunchFailure> {
        let row = &input_0.row();
        let intent = &input_1.intent();
        let every_part =
            usize::try_from(self.seen_parts).ok() == Some(intent.prompt_sha256_vector.len());
        let kind = row.get("type").and_then(serde_json::Value::as_str);
        match intent.harness_kind {
            HarnessKind::Claude => {
                let exact_session = row
                    .get("sessionId")
                    .or_else(|| row.get("session_id"))
                    .and_then(serde_json::Value::as_str)
                    == Some(intent.native_session_id.as_str());
                if !exact_session {
                    return Ok(None);
                }
                // The effective leave-outs were applied: no tool they remove
                // is used, and no skill listing is shown when the Skill tool
                // is left out.
                let lean = LeanStart {
                    harness_kind: intent.harness_kind.clone(),
                    leave_outs: intent.leave_outs.clone(),
                    tool_set: signal_flow::ToolSet::Default,
                };
                let removed = lean.disallowed_tools();
                let used_removed = row
                    .pointer("/message/content")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|contents| {
                        contents.iter().any(|content| {
                            content.get("type").and_then(serde_json::Value::as_str)
                                == Some("tool_use")
                                && content
                                    .get("name")
                                    .and_then(serde_json::Value::as_str)
                                    .is_some_and(|name| removed.iter().any(|tool| tool == name))
                        })
                    });
                let listed_skills = lean.skill_listing_off()
                    && row
                        .pointer("/attachment/type")
                        .and_then(serde_json::Value::as_str)
                        == Some("skill_listing");
                if used_removed || listed_skills {
                    return Err(NativeLaunchFailure::LeaveOutShown);
                }
                if kind == Some("user")
                    && !every_part
                    && let Some(text) = row.claude_typed_text()
                {
                    let original = usize::try_from(self.seen_parts)
                        .ok()
                        .and_then(|seen| intent.prompt_sha256_vector.get(seen))
                        .and_then(|part_sha256| text.claude_pasted_part(part_sha256))
                        .unwrap_or(text);
                    self.read_part(&original, intent)?;
                    return Ok(None);
                }
                if kind == Some("assistant") && self.seen_parts > 0 {
                    if every_part {
                        if row
                            .pointer("/message/model")
                            .and_then(serde_json::Value::as_str)
                            != Some(intent.model_name.as_str())
                            || !row.claude_effort_matches(intent.effort.as_str())
                        {
                            return Err(NativeLaunchFailure::ModelOrEffortDiffers);
                        }
                        return Ok(row.assistant_turn(&intent.native_session_id));
                    }
                    self.last_part_answered = true;
                }
                Ok(None)
            }
            HarnessKind::Codex => {
                let payload = row.get("payload");
                let payload_kind = payload
                    .and_then(|payload| payload.get("type"))
                    .and_then(serde_json::Value::as_str);
                // A block the launch left out in a request Codex made is a
                // launch that did not start lean: it is refused.
                if kind == Some("response_item")
                    && payload_kind == Some("message")
                    && matches!(
                        payload
                            .and_then(|payload| payload.get("role"))
                            .and_then(serde_json::Value::as_str),
                        Some("user" | "developer")
                    )
                {
                    let texts = payload
                        .and_then(|payload| payload.get("content"))
                        .and_then(serde_json::Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter_map(|item| item.get("text").and_then(serde_json::Value::as_str));
                    for text in texts {
                        if CODEX_LEFT_OUT_OPENINGS.iter().any(|(leave_out, opening)| {
                            intent.leave_outs.contains(leave_out) && text.starts_with(opening)
                        }) {
                            return Err(NativeLaunchFailure::LeaveOutShown);
                        }
                    }
                }
                if kind == Some("turn_context") {
                    let payload = payload.ok_or(NativeLaunchFailure::TranscriptUnreadable)?;
                    if payload.get("model").and_then(serde_json::Value::as_str)
                        != Some(intent.model_name.as_str())
                        || payload.get("effort").and_then(serde_json::Value::as_str)
                            != Some(intent.effort.as_str())
                    {
                        return Err(NativeLaunchFailure::ModelOrEffortDiffers);
                    }
                    self.native_turn_option = payload
                        .get("turn_id")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned);
                    return Ok(None);
                }
                if kind == Some("event_msg")
                    && row
                        .pointer("/payload/thread_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(intent.native_session_id.as_str())
                    && row
                        .pointer("/payload/item/type")
                        .and_then(serde_json::Value::as_str)
                        == Some("UserMessage")
                {
                    let turn = row
                        .pointer("/payload/turn_id")
                        .and_then(serde_json::Value::as_str);
                    let content = row
                        .pointer("/payload/item/content")
                        .and_then(serde_json::Value::as_array)
                        .ok_or(NativeLaunchFailure::PartDiffers)?;
                    let text = match content.as_slice() {
                        [item]
                            if item.get("type").and_then(serde_json::Value::as_str)
                                == Some("text") =>
                        {
                            item.get("text").and_then(serde_json::Value::as_str)
                        }
                        _ => None,
                    }
                    .ok_or(NativeLaunchFailure::PartDiffers)?;
                    if every_part || turn.is_none() || turn != self.native_turn_option.as_deref() {
                        return Err(NativeLaunchFailure::PartDiffers);
                    }
                    self.read_part(text, intent)?;
                    self.part_turn_option = turn.map(str::to_owned);
                    return Ok(None);
                }
                if kind == Some("event_msg")
                    && payload_kind == Some("task_complete")
                    && self.seen_parts > 0
                    && row
                        .pointer("/payload/turn_id")
                        .and_then(serde_json::Value::as_str)
                        == self.part_turn_option.as_deref()
                {
                    self.last_part_answered = true;
                    return Ok(None);
                }
                // The flow's first answer: the first model output of the
                // last part's turn, never a user or developer message.
                if every_part
                    && kind == Some("response_item")
                    && !matches!(
                        payload
                            .and_then(|payload| payload.get("role"))
                            .and_then(serde_json::Value::as_str),
                        Some("user" | "developer" | "system")
                    )
                {
                    return Ok(self.part_turn_option.clone());
                }
                Ok(None)
            }
        }
    }
}

impl ObservesNativeTargetReceipt for HerdrCli {
    fn observe_native_target_receipt(
        &self,
        durable_intent: &PromptDeliveryIntent,
    ) -> Result<StartupProgress, TranscriptRefusal> {
        let Some((input, adopted_after_absence)) = self.receipt_input(durable_intent)? else {
            return Ok(StartupProgress::Waiting(durable_intent.clone()));
        };
        let mut reading = StartupReading {
            seen_parts: 0,
            last_part_answered: false,
            native_turn_option: None,
            part_turn_option: None,
        };
        let mut observed = None;
        let mut reader = BufReader::new(input);
        // A Codex transcript is read from its start: its first record must
        // name exactly the thread bound, before any user turn counts.
        let mut thread_unnamed = durable_intent.harness_kind == HarnessKind::Codex
            && match &durable_intent.native_transcript_boundary {
                NativeTranscriptBoundary::Absent(_) => true,
                NativeTranscriptBoundary::Existing(cursor) => cursor.transcript_byte_offset == 0,
            };
        loop {
            let mut record = Vec::new();
            let read = reader
                .read_until(b'\n', &mut record)
                .map_err(|error| format!("native transcript read failed: {error}"))?;
            if read == 0 || record.last() != Some(&b'\n') {
                break;
            }
            let Ok(row) = serde_json::from_slice::<serde_json::Value>(&record) else {
                continue;
            };
            if thread_unnamed {
                if !row.names_thread(&durable_intent.native_session_id) {
                    return Err(NativeLaunchFailure::ThreadNotNamed.into());
                }
                thread_unnamed = false;
            }
            if let Some(turn) = reading.read_record(&row, durable_intent)? {
                observed = Some((turn, format!("{:x}", Sha256::digest(&record))));
                break;
            }
        }
        if let Some((native_turn_id, receipt_sha256)) = observed {
            return Ok(StartupProgress::Received(NativeTargetReceipt {
                launch_request_id: durable_intent.launch_request_id.clone(),
                prompt_sha256: durable_intent.prompt_sha256.clone(),
                flow_id: durable_intent.flow_id.clone(),
                native_session_id: durable_intent.native_session_id.clone(),
                native_turn_id,
                receipt_sha256,
                model_name: durable_intent.model_name.clone(),
                effort: durable_intent.effort.clone(),
                prompt_sha256_vector: durable_intent.prompt_sha256_vector.clone(),
            }));
        }
        let mut intent = durable_intent.clone();
        if adopted_after_absence {
            let binding = NativeLaunchBinding {
                launch_request_id: durable_intent.launch_request_id.clone(),
                flow_id: durable_intent.flow_id.clone(),
                native_session_id: durable_intent.native_session_id.clone(),
                harness_kind: durable_intent.harness_kind.clone(),
                herdr_pane_binding: durable_intent.herdr_pane_binding.clone(),
            };
            // Preserve the observed inode but rescan from byte zero on
            // reconciliation. Advancing to EOF here would discard parts
            // emitted before the receipt.
            let boundary =
                self.capture_transcript_start_boundary(&binding, &durable_intent.model_name)?;
            if !matches!(boundary, NativeTranscriptBoundary::Existing(_)) {
                return Err("new native transcript disappeared during adoption"
                    .to_owned()
                    .into());
            }
            intent.native_transcript_boundary = boundary;
            // The adopted cursor is recorded before any part advances.
            return Ok(StartupProgress::Waiting(intent));
        }
        if reading.seen_parts == durable_intent.parts_submitted
            && reading.last_part_answered
            && usize::try_from(durable_intent.parts_submitted)
                .is_ok_and(|submitted| submitted < durable_intent.prompt_sha256_vector.len())
        {
            return Ok(StartupProgress::PartAnswered(intent));
        }
        Ok(StartupProgress::Waiting(intent))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::{
        AcceptsLaunchRegistration, CreatesHerdrLaunchPane, ObservesNativeLaunchBinding,
        ObservesNativeTargetReceipt, StartsNativeHerdrHarness, StartupProgress, SubmitsStartupPart,
        TitlesNativeFlow,
    };
    use crate::composition::{FitsOneMessage, SplitsStartupMessage};
    use crate::fixture_executable::{FixtureExecutable, InstallsScript};
    use crate::herdr::ConfiguresHerdrCli;
    use crate::herdr::HerdrCli;
    use crate::herdr::launch::ChecksComposedLaunch;
    use crate::herdr::launch::ClaimsNativeIdentity;
    use crate::herdr::launch::PreparesClaudeEnvironment;
    use crate::herdr::launch::PreparesClaudePane;
    use crate::herdr::launch::QuotesForShell;
    use crate::herdr::launch::ReadsLaunchText;
    use crate::herdr::launch::TitlesClaudeSession;
    use crate::herdr::launch::{ChecksTypedCommand, HoldsSystemPrompt, SpawnFailure};
    use crate::herdr::launch::{NativeTargetRefusal, TitleRefusal};
    use crate::title::ShowsNativeTitle;
    use signal_flow::{
        Aspect, ComposedLaunch, Effort, FirstPromptPayload, HarnessKind, HerdrPaneBinding,
        LaunchProfile, ModelName, NativeLaunchBinding, NativeLaunchFailure, NativeTargetReceipt,
        NativeTranscriptBoundary, PromptDeliveryIntent, PromptDeliveryResult, PromptPart,
        RegistrationAcknowledgement, SystemPrompt, TargetReceiptRequest,
    };
    use std::fs;
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixListener;
    use std::path::Path;

    const SYSTEM_TEXT: &str = "Metaflow: { Field flow Secondary }\nModel: model-current\nEffort: high\nFlow ID: pending\nNative binding: pending\nfixture system prompt";

    fn digest(text: &str) -> String {
        format!(
            "{:x}",
            <sha2::Sha256 as sha2::Digest>::digest(text.as_bytes())
        )
    }

    const PROMPT_HASH: &str = "0cb26cfe0a554e4780aa5af20cafbe3ae3259f823438576026a4ffff58371a67";

    pub(crate) fn launch(harness_kind: HarnessKind) -> ComposedLaunch {
        ComposedLaunch {
            harness_kind,
            launch_profile: LaunchProfile {
                launch_request_id: "launch-42".into(),
                system_modules: vec!["spirit-core".into(), "intent-flow".into()],
                user_modules: vec!["operation-main-flow".into()],
                address: signal_flow::Address {
                    aspect: Aspect::Field,
                    topic: "flow".into(),
                    layer: signal_flow::Layer::Secondary,
                },
                model_name: ModelName::from("model-current"),
                effort: Effort::from("high"),
                flow_id_option: None,
                remembering: signal_flow::Remembering::Predecessors(0),
                herdr_session_name: "flowlaunch42".into(),
                leave_outs_option: None,
                instruction_prompt: "do the work".into(),
                tool_set: signal_flow::ToolSet::Default,
            },
            system_prompt: SystemPrompt {
                system_prompt_text: SYSTEM_TEXT.into(),
                system_prompt_sha256: digest(SYSTEM_TEXT),
            },
            first_prompt_payload: FirstPromptPayload {
                first_prompt_body: "composed body".into(),
                prompt_sha256: PROMPT_HASH.into(),
                prompt_part_vector: vec![PromptPart {
                    prompt_part_text: "composed body".into(),
                    prompt_sha256: PROMPT_HASH.into(),
                }],
            },
            target_receipt_request: TargetReceiptRequest {
                launch_request_id: "launch-42".into(),
                prompt_sha256: PROMPT_HASH.into(),
            },
            leave_outs: Vec::new(),
        }
    }

    /// A launch whose first message travels in two parts.
    fn two_part_launch(harness_kind: HarnessKind) -> ComposedLaunch {
        let chunks = ["first module text\n", "second module text\n\ndo the work"];
        let mut launch = launch(harness_kind);
        let body = chunks.concat();
        launch.first_prompt_payload.prompt_sha256 = digest(&body);
        launch.target_receipt_request.prompt_sha256 = digest(&body);
        launch.first_prompt_payload.first_prompt_body = body;
        launch.first_prompt_payload.prompt_part_vector = chunks
            .iter()
            .enumerate()
            .map(|(index, chunk)| {
                let text = format!("{}{chunk}", HarnessKind::part_header((index + 1, 2)));
                PromptPart {
                    prompt_sha256: digest(&text),
                    prompt_part_text: text,
                }
            })
            .collect();
        launch
    }

    pub(crate) fn fixture_herdr(
        harness: &str,
        native_session: &str,
        claimed_identity: &str,
        agent_name: &str,
    ) -> (tempfile::TempDir, HerdrCli) {
        let root = tempfile::tempdir().expect("fixture root");
        let executable = root.path().join("herdr");
        let calls = root.path().join("calls");
        let reported_harness = root.path().join("reported-harness");
        fs::write(&reported_harness, harness).expect("reported harness");
        let script = format!(
            r##"#!/bin/sh
printf '%s\n' "$*" >> '{calls}'
case "$*" in
  *"workspace create"*) printf '%s\n' '{{"result":{{"workspace":{{"workspace_id":"w7"}},"root_pane":{{"pane_id":"w7:p1","terminal_id":"term-native"}}}}}}' ;;
  *"pane run"*) command=$6; case "$command" in
      *"FLOW_CLAUDE_ENV_READY_"*) sh -c "$command" > '{marker_output}'; width=$(cat '{pane_width}' 2>/dev/null || printf 120); fold -w "$width" '{marker_output}' > '{visible_output}' ;;
      *) [ -f '{start_refused}' ] && {{ cat '{start_refused}' >&2; exit 1; }}; printf '%s' "$command" > '{owned_command}'; owned=${{command#*exec }}; eval "set -- $owned"; prev=; for a in "$@"; do [ "$prev" = --system-prompt-file ] && cat "$a" > '{system_read}'; if [ "$prev" = --name ]; then title=$a; [ -f '{title_override}' ] && title=$(cat '{title_override}'); printf '%s' "$title" > '{title_file}'; record=$(printf '{{"type":"custom-title","customTitle":"%s","sessionId":"{native_session}"}}' "$title"); if [ -f '{record_delay}' ]; then printf '%s\n' "$record" > '{pending_record}'; else printf '%s\n' "$record" >> '{claude_transcript}'; fi; fi; prev=$a; done ;;
    esac ;;
  *"pane wait-output"*) case "$*" in *"--source recent-unwrapped"*) ;; *) exit 9 ;; esac; marker=$(printf '%s\n' "$*" | sed 's/.*--match \([^ ]*\).*/\1/'); line=$(grep -Fx "$marker" '{marker_output}') || exit 10; printf '{{"result":{{"pane_id":"w7:p1","matched_line":"%s"}}}}\n' "$line" ;;
  *"agent wait"*) [ -f '{wait_refused}' ] && exit 11; rm -f '{missing_agent_session}'; printf '%s\n' '{{"result":{{"status":"idle"}}}}' ;;
  *"agent start"*) [ -f '{start_refused}' ] && {{ cat '{start_refused}' >&2; exit 1; }}; prev=; for a in "$@"; do [ "$prev" = --system-prompt-file ] && cat "$a" > '{system_read}'; if [ "$prev" = --name ]; then title=$a; [ -f '{title_override}' ] && title=$(cat '{title_override}'); printf '%s' "$title" > '{title_file}'; record=$(printf '{{"type":"custom-title","customTitle":"%s","sessionId":"{native_session}"}}' "$title"); if [ -f '{record_delay}' ]; then printf '%s\n' "$record" > '{pending_record}'; else printf '%s\n' "$record" >> '{claude_transcript}'; fi; fi; prev=$a; done; printf '%s\n' '{{"result":{{"agent":{{"name":"{agent_name}"}}}}}}' ;;
  *"agent get"*) if [ -f '{pending_record}' ]; then left=$(cat '{record_delay}'); if [ "$left" -le 0 ]; then cat '{pending_record}' >> '{claude_transcript}'; rm -f '{pending_record}'; else printf '%s' $((left - 1)) > '{record_delay}'; fi; fi; reported_harness=$(cat '{reported_harness}'); title=$(cat '{title_file}' 2>/dev/null); [ "$reported_harness" = claude ] && source=flow || source=herdr:$reported_harness; if [ -f '{missing_agent_session}' ] || [ -f '{never_agent_session}' ]; then printf '{{"result":{{"agent":{{"name":"{agent_name}","agent":"%s","agent_status":"idle","workspace_id":"w7","pane_id":"w7:p1","terminal_id":"term-native","interactive_ready":true,"terminal_title_stripped":"%s"}}}}}}\n' "$reported_harness" "$title"; else printf '{{"result":{{"agent":{{"name":"{agent_name}","agent":"%s","agent_status":"idle","workspace_id":"w7","pane_id":"w7:p1","terminal_id":"term-native","interactive_ready":true,"terminal_title_stripped":"%s","agent_session":{{"source":"%s","agent":"%s","kind":"id","value":"{native_session}"}}}}}}}}\n' "$reported_harness" "$title" "$source" "$reported_harness"; fi ;;
  *"pane process-info"*) command=$(cat '{owned_command}'); session=$(printf '%s' "$command" | sed "s/.*'--session-id' '\([^']*\)'.*/\1/"); model=$(printf '%s' "$command" | sed "s/.*'--model' '\([^']*\)'.*/\1/"); printf '{{"result":{{"process_info":{{"pane_id":"w7:p1","foreground_processes":[{{"pid":42,"name":"claude","argv":["/fixture/claude","--session-id","%s","--model","%s"]}}]}}}}}}\n' "$session" "$model" ;;
  *"agent prompt"*) printf '%s\n' '{{"result":{{"accepted":true}}}}' ;;
  *"pane rename w7:p1 "*) printf '%s' "$*" | sed 's/.*pane rename w7:p1 //' > '{label_file}'; printf '%s\n' '{{"result":{{}}}}' ;;
  *"pane report-metadata w7:p1 "*) [ -f '{display_refused}' ] && exit 14; printf '%s' "$*" | sed 's/.*--display-agent //' > '{display_file}'; printf '%s\n' '{{"result":{{}}}}' ;;
  *"pane get w7:p1"*) label=$(cat '{label_file}' 2>/dev/null); display=$(cat '{display_file}' 2>/dev/null); printf '{{"result":{{"pane":{{"pane_id":"w7:p1","terminal_id":"term-native","label":"%s","display_agent":"%s"}}}}}}\n' "$label" "$display" ;;
  *"workspace rename w7 "*) [ -f '{workspace_refused}' ] && exit 12; printf '%s' "$*" | sed 's/.*workspace rename w7 //' > '{workspace_label}'; printf '%s\n' '{{"result":{{}}}}' ;;
  *"workspace get w7"*) label=$(cat '{workspace_label}' 2>/dev/null); printf '{{"result":{{"workspace":{{"workspace_id":"w7","label":"%s"}}}}}}\n' "$label" ;;
  "session list --json") printf '%s\n' '{{"sessions":[{{"name":"flowlaunch42","socket_path":"{herdr_socket}"}}]}}' ;;
  *) exit 8 ;;
esac
"##,
            calls = calls.display(),
            start_refused = root.path().join("agent-start-refused").display(),
            system_read = root.path().join("system-prompt-read").display(),
            reported_harness = reported_harness.display(),
            missing_agent_session = root.path().join("missing-agent-session").display(),
            never_agent_session = root.path().join("never-agent-session").display(),
            wait_refused = root.path().join("agent-wait-refused").display(),
            marker_output = root.path().join("marker-unwrapped").display(),
            owned_command = root.path().join("owned-command").display(),
            visible_output = root.path().join("marker-visible").display(),
            pane_width = root.path().join("pane-width").display(),
            title_file = root.path().join("native-title").display(),
            title_override = root.path().join("native-title-override").display(),
            record_delay = root.path().join("title-record-delay").display(),
            pending_record = root.path().join("title-record-pending").display(),
            label_file = root.path().join("pane-label").display(),
            display_file = root.path().join("pane-display-agent").display(),
            display_refused = root.path().join("display-agent-refused").display(),
            workspace_label = root.path().join("workspace-label").display(),
            workspace_refused = root.path().join("workspace-rename-refused").display(),
            herdr_socket = root.path().join("herdr.sock").display(),
            claude_transcript = root
                .path()
                .join("native-transcripts/claude")
                .join(format!("{native_session}.jsonl"))
                .display(),
        );
        FixtureExecutable {
            path: executable.clone(),
        }
        .install(&script);
        // The fixture Herdr server's socket: each `agent.prompt` request
        // line is kept, and answered as Herdr 0.8.2 answers it.
        let listener = UnixListener::bind(root.path().join("herdr.sock")).expect("Herdr socket");
        let prompts = root.path().join("socket-prompts");
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else {
                    return;
                };
                let mut line = String::new();
                if BufReader::new(&stream).read_line(&mut line).is_err() {
                    continue;
                }
                let _ = fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&prompts)
                    .and_then(|mut file| file.write_all(line.as_bytes()));
                let _ = stream.write_all(
                    b"{\"id\":\"flow:prompt\",\"result\":{\"type\":\"agent_prompted\"}}\n",
                );
            }
        });
        let flows = root.path().join("flows");
        fs::create_dir(&flows).expect("fixture flows root");
        let transcript_root = root.path().join("native-transcripts");
        fs::create_dir_all(transcript_root.join("codex")).expect("codex transcript root");
        fs::create_dir_all(transcript_root.join("claude")).expect("claude transcript root");
        let flow_id = transcript_root.join("flow-id");
        let flow_id_calls = root.path().join("flow-id-calls");
        let uuid_version = match (harness, claimed_identity.as_bytes().get(12)) {
            ("claude", Some(b'5')) => "uuid-version=uuid-v5\n",
            ("claude", _) => "uuid-version=uuid-v4\n",
            _ => "",
        };
        // The claimed alias is 123456 unless the root's `flow-id-alias`
        // names another.
        let flow_id_script = format!(
            r##"#!/bin/sh
printf '%s|%s\n' "$*" "$CODEX_SESSION_ID" >> '{flow_id_calls}'
alias=$(cat '{alias_file}' 2>/dev/null || printf 123456)
printf 'version=1\nharness={harness}\nidentity={claimed_identity}\nalias=%s\n{uuid_version}' "$alias" > '{flows}/.'"$alias"'.flow-id'
printf '%s\n' "$alias"
"##,
            flow_id_calls = flow_id_calls.display(),
            alias_file = root.path().join("flow-id-alias").display(),
            flows = flows.display(),
        );
        FixtureExecutable {
            path: flow_id.clone(),
        }
        .install(&flow_id_script);
        let readiness_runtime = root.path().join("native-transcripts");
        let adapter = HerdrCli::at(executable, flows);
        let owned_command = root.path().join("owned-command");
        std::thread::spawn(move || {
            for _ in 0..2000 {
                if let Ok(entries) = fs::read_dir(&readiness_runtime) {
                    for entry in entries.flatten() {
                        if entry
                            .file_name()
                            .to_string_lossy()
                            .starts_with("claude-ready-")
                        {
                            let launched = fs::read_to_string(&owned_command)
                                .is_ok_and(|command| command.contains("--session-id"));
                            if let (true, Ok(sender)) =
                                (launched, std::os::unix::net::UnixDatagram::unbound())
                            {
                                let _ = sender.send_to(&[], entry.path());
                                return;
                            }
                        }
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        });
        (root, adapter)
    }

    fn registered_intent(
        adapter: &HerdrCli,
        launch: &ComposedLaunch,
        pane: signal_flow::HerdrPaneBinding,
        native_session: &str,
    ) -> PromptDeliveryIntent {
        registration_of(adapter, launch, pane, native_session).expect("registered prompt intent")
    }

    fn report_fixture_readiness(adapter: &HerdrCli, pane: &HerdrPaneBinding) {
        let path = adapter
            .flow_owned_claude_readiness
            .lock()
            .unwrap()
            .get(&pane.herdr_pane_id)
            .map(|(_, path)| path.clone())
            .expect("fixture readiness endpoint");
        std::os::unix::net::UnixDatagram::unbound()
            .unwrap()
            .send_to(&[], path)
            .unwrap();
    }

    fn registration_of(
        adapter: &HerdrCli,
        launch: &ComposedLaunch,
        pane: signal_flow::HerdrPaneBinding,
        native_session: &str,
    ) -> Result<PromptDeliveryIntent, super::RegistrationRefusal> {
        let binding = NativeLaunchBinding {
            launch_request_id: launch.launch_profile.launch_request_id.clone(),
            flow_id: "123456".into(),
            native_session_id: native_session.into(),
            harness_kind: launch.harness_kind.clone(),
            herdr_pane_binding: pane.clone(),
        };
        let acknowledgement = RegistrationAcknowledgement {
            launch_request_id: binding.launch_request_id.clone(),
            flow_id: binding.flow_id.clone(),
            native_session_id: binding.native_session_id.clone(),
            herdr_pane_binding: pane,
        };
        adapter.accept_registration(launch, &binding, &acknowledgement)
    }

    /// The `agent.prompt` request lines the fixture Herdr socket received.
    fn socket_prompts(root: &tempfile::TempDir) -> Vec<serde_json::Value> {
        fs::read_to_string(root.path().join("socket-prompts"))
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).expect("a JSON request line"))
            .collect()
    }

    #[test]
    fn codex_launch_selects_one_exact_executable_and_remote_socket() {
        let launch = launch(HarnessKind::Codex);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "codex",
            "12345678-1234-4abc-8def-123456789abc",
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        adapter
            .start_native_harness(&launch, &pane, None)
            .expect("started exact Codex client");
        let calls = fs::read_to_string(root.path().join("calls")).expect("launch calls");
        let start = calls
            .lines()
            .find(|line| line.contains("agent start"))
            .expect("agent start call");
        assert!(start.contains("--executable /fixture/codex"));
        assert!(start.contains("-- --remote unix:///tmp/stable-codex.sock --model model-current"));
        assert!(start.contains("-c model_reasoning_effort=high"));
        // The thread holds the system prompt as its base instructions; the
        // TUI is not given it in any form.
        assert!(!start.contains("instructions="));
        assert!(!start.contains(SYSTEM_TEXT));
        assert!(!start.contains("model_instructions_file"));
        assert!(!start.contains("--remote-control"));
        assert_eq!(start.matches("--remote").count(), 1);
        assert!(!calls.contains("pane run"));
        assert!(!start.contains(HerdrCli::CLAUDE_SKIP_PERMISSIONS_FLAG));
        // The harness starts with no positional prompt; the one first prompt
        // travels later in a single app-server turn/start.
        assert!(
            start.ends_with(
                "-c model_reasoning_effort=high -c tui.terminal_title=[\"thread-title\"]"
            )
        );
        assert!(!start.contains('$'));
        assert!(!calls.contains("agent prompt"));
    }

    /// A Claude system prompt of many lines, tabs and quotes is never typed:
    /// the start names the Nexus's in-memory file, the harness reads the
    /// text whole from it, and the typed command carries no control
    /// character.
    #[test]
    fn a_multiline_claude_system_prompt_is_read_whole_from_the_held_file() {
        let mut launch = launch(HarnessKind::Claude);
        let text = "First line.\n\tIndented 'quoted' line.\n\nLast line with \"quotes\".\n";
        launch.system_prompt.system_prompt_text = text.into();
        launch.system_prompt.system_prompt_sha256 = digest(text);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            "12345678-1234-4abc-8def-123456789abc",
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        adapter
            .start_native_harness(&launch, &pane, Some("123456"))
            .expect("a multiline system prompt starts");
        assert_eq!(
            fs::read_to_string(root.path().join("system-prompt-read")).unwrap(),
            text
        );
        let start = fs::read_to_string(root.path().join("owned-command"))
            .expect("one owned command: no argument broke it");
        assert!(!start.contains("First line."));
        assert!(!start.contains("Last line"));
    }

    /// An argument carrying a control character is refused by Flow before
    /// Herdr is called, and the held system prompt is closed.
    #[test]
    fn an_argument_with_a_control_character_is_refused_before_herdr() {
        let mut launch = launch(HarnessKind::Claude);
        launch.launch_profile.effort = "high\nmedium".into();
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            "12345678-1234-4abc-8def-123456789abc",
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        assert_eq!(
            adapter.start_native_harness(&launch, &pane, Some("123456")),
            Err(SpawnFailure::Refused(
                signal_flow::SpawnRefusalCause::ControlCharacter
            ))
        );
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        assert!(!calls.contains("agent start"), "Herdr was not called");
        assert_eq!(
            adapter.held_system_prompt_path(&launch.launch_profile.launch_request_id),
            None
        );
    }

    /// A typed command longer than one terminal line is refused by Flow
    /// before Herdr is called.
    #[test]
    fn a_typed_command_longer_than_one_terminal_line_is_refused_before_herdr() {
        let mut launch = launch(HarnessKind::Claude);
        launch.launch_profile.effort = "e".repeat(4096);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            "12345678-1234-4abc-8def-123456789abc",
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        assert_eq!(
            adapter.start_native_harness(&launch, &pane, Some("123456")),
            Err(SpawnFailure::Refused(
                signal_flow::SpawnRefusalCause::CommandTooLong
            ))
        );
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        assert!(!calls.contains("agent start"), "Herdr was not called");
    }

    /// The typed line is measured as Herdr quotes it for a POSIX shell.
    #[test]
    fn the_typed_command_is_measured_as_herdr_quotes_it() {
        assert_eq!(<[String]>::quoted_length("--model"), 7);
        assert_eq!(<[String]>::quoted_length("two words"), 11);
        assert_eq!(<[String]>::quoted_length("a'b"), 8);
        assert_eq!(<[String]>::quoted_length(""), 2);
        let fits = ["claude".to_owned(), "x".repeat(4095 - 7)];
        assert_eq!(fits.typed_command_refusal(), None);
        let longer = ["claude".to_owned(), "x".repeat(4095 - 6)];
        assert_eq!(
            longer.typed_command_refusal(),
            Some(signal_flow::SpawnRefusalCause::CommandTooLong)
        );
        let tab = ["claude".to_owned(), "a\tb".to_owned()];
        assert_eq!(
            tab.typed_command_refusal(),
            Some(signal_flow::SpawnRefusalCause::ControlCharacter)
        );
    }

    /// Herdr's refusal is kept as Herdr wrote it: the code and message of its
    /// JSON error body, or no code and its whole text.
    #[test]
    fn herdrs_refusal_is_kept_as_herdr_wrote_it() {
        let launch = launch(HarnessKind::Claude);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            "12345678-1234-4abc-8def-123456789abc",
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        fs::write(
            root.path().join("agent-start-refused"),
            r#"{"error":{"code":"invalid_agent_argument","message":"agent arguments cannot be encoded safely for the target shell"},"id":"cli:agent:start"}"#,
        )
        .unwrap();
        assert_eq!(
            adapter.start_native_harness(&launch, &pane, Some("123456")),
            Err(SpawnFailure::Refused(
                signal_flow::SpawnRefusalCause::Herdr(signal_flow::HerdrRefusal {
                    herdr_error_code_option: Some("invalid_agent_argument".into()),
                    herdr_error_message:
                        "agent arguments cannot be encoded safely for the target shell".into(),
                })
            ))
        );
        assert_eq!(
            adapter.held_system_prompt_path(&launch.launch_profile.launch_request_id),
            None,
            "a refused start closes the held system prompt"
        );
        fs::write(
            root.path().join("agent-start-refused"),
            "missing value for --kind\n",
        )
        .unwrap();
        assert_eq!(
            adapter.start_native_harness(&launch, &pane, Some("123456")),
            Err(SpawnFailure::Refused(
                signal_flow::SpawnRefusalCause::Herdr(signal_flow::HerdrRefusal {
                    herdr_error_code_option: None,
                    herdr_error_message: "missing value for --kind".into(),
                })
            ))
        );
    }

    #[test]
    fn a_claude_start_carries_its_leave_outs_as_flags_environment_and_settings() {
        use signal_flow::LeaveOut;
        let mut launch = launch(HarnessKind::Claude);
        launch.leave_outs = vec![
            LeaveOut::SkillTool,
            LeaveOut::WorkflowTool,
            LeaveOut::GitInstructions,
            LeaveOut::AttributionReminder,
        ];
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            "12345678-1234-4abc-8def-123456789abc",
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        adapter
            .start_native_harness(&launch, &pane, Some("123456"))
            .expect("started");
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        let start = fs::read_to_string(root.path().join("owned-command")).unwrap();
        assert!(start.contains("'--disable-slash-commands'"));
        assert!(start.contains("'--disallowedTools' 'Skill,Workflow'"));
        assert!(start.contains(r#""attribution":{"commit":"","pr":""}"#));
        let prepared = calls
            .lines()
            .find(|line| line.contains("pane run"))
            .expect("pane prepared");
        assert!(prepared.contains("export 'CLAUDE_CODE_DISABLE_WORKFLOWS=1'"));
        assert!(prepared.contains("export 'CLAUDE_CODE_DISABLE_GIT_INSTRUCTIONS=1'"));
    }

    #[test]
    fn a_receipt_showing_a_removed_tool_refuses_the_launch() {
        use signal_flow::LeaveOut;
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let mut launch = launch(HarnessKind::Claude);
        launch.leave_outs = vec![LeaveOut::SkillTool, LeaveOut::WorkflowTool];
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        let transcript = root
            .path()
            .join("native-transcripts/claude")
            .join(format!("{native_session}.jsonl"));
        fs::write(&transcript, "{\"type\":\"session_meta\"}\n").unwrap();
        let intent = registered_intent(&adapter, &launch, pane, native_session);
        assert_eq!(intent.leave_outs, launch.leave_outs);
        let user = serde_json::json!({"type":"user","sessionId":native_session,
            "message":{"role":"user","content":launch.first_prompt_payload.first_prompt_body}});
        let skill_call = serde_json::json!({"type":"assistant","sessionId":native_session,
            "uuid":"turn-skill","effort":"high","message":{"model":"model-current",
            "content":[{"type":"tool_use","id":"t1","name":"Skill","input":{"skill":"x"}}]}});
        let mut append = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        writeln!(append, "{user}\n{skill_call}").unwrap();
        assert!(
            adapter.observe_native_target_receipt(&intent).unwrap_err()
                == super::TranscriptRefusal::Contradicted(NativeLaunchFailure::LeaveOutShown)
        );
    }

    #[test]
    fn claude_environment_preparation_removes_inherited_claude_identity() {
        let checks = HerdrCli::CLAUDE_INHERITED_ENVIRONMENT
            .iter()
            .map(|name| format!("if [ \"${{{name}+present}}\" = present ]; then exit 19; fi"))
            .collect::<Vec<_>>()
            .join("; ");
        assert!(HerdrCli::CLAUDE_INHERITED_ENVIRONMENT.contains(&"CLAUDE_JOB_DIR"));
        let mut command = std::process::Command::new("sh");
        command.arg("-c").arg(format!(
            "{}; {checks}",
            ("test-marker").claude_environment_preparation(
                None,
                Path::new("/run/flow.sock"),
                Path::new("/fixture/herdr"),
                "fixture-session",
                &[],
            ),
        ));
        for name in HerdrCli::CLAUDE_INHERITED_ENVIRONMENT {
            command.env(name, "/home/li/.claude/jobs/108ab020");
        }
        let output = command.output().expect("shell environment witness");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            String::from_utf8(output.stdout).expect("UTF-8 marker"),
            "FLOW_CLAUDE_ENV_READY_test-marker\n"
        );
    }

    /// The FlowId Reserve claimed reaches the harness's environment: the
    /// line typed before the harness removes an inherited FLOW_ID and
    /// exports the reserved one, which a child of that shell (the harness,
    /// and every hook it runs) inherits.
    #[test]
    fn claude_environment_preparation_exports_the_reserved_flow_id() {
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!(
                "{}; sh -c 'printf %s \"$FLOW_ID\"'",
                ("test-marker").claude_environment_preparation(
                    Some("5a4d0b"),
                    Path::new("/run/flow.sock"),
                    Path::new("/fixture/herdr"),
                    "fixture-session",
                    &[]
                ),
            ))
            .env("FLOW_ID", "0a0a0a")
            .output()
            .expect("shell environment witness");
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8(output.stdout).expect("UTF-8"),
            "FLOW_CLAUDE_ENV_READY_test-marker\n5a4d0b"
        );
    }

    /// The launching Nexus's ordinary socket reaches the harness's
    /// environment as FLOW_SOCKET, replacing one the pane's shell
    /// inherited, as one word however the path is spelled.
    #[test]
    fn claude_environment_preparation_exports_the_launching_nexus_socket() {
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg(format!(
                "{}; sh -c 'printf %s \"$FLOW_SOCKET\"'",
                ("test-marker").claude_environment_preparation(
                    None,
                    Path::new("/run/user/1001/flow-next/it's a $HOME/flow.sock"),
                    Path::new("/fixture/herdr"),
                    "fixture-session",
                    &[]
                ),
            ))
            .env("FLOW_SOCKET", "/run/user/1001/flow/flow.sock")
            .output()
            .expect("shell environment witness");
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8(output.stdout).expect("UTF-8"),
            "FLOW_CLAUDE_ENV_READY_test-marker\n/run/user/1001/flow-next/it's a $HOME/flow.sock"
        );
    }

    #[test]
    fn a_reserved_claude_launch_starts_as_its_session_with_its_flow_id() {
        use crate::herdr::reservation::{ChoosesNativeSession, ReservesFlowIdentity};
        let launch = launch(HarnessKind::Claude);
        let session = HerdrCli::default()
            .with_ordinary_socket(Path::new("/fixture/run/flow-next/flow/flow.sock"))
            .reserved_native_session_id(&launch)
            .expect("a Claude launch chooses its session");
        let agent_name = launch.launch_agent_name();
        let (root, adapter) =
            fixture_herdr("claude", &session, &session.replace('-', ""), &agent_name);
        assert_eq!(
            adapter.reserve_flow_identity(&launch).unwrap().as_deref(),
            Some("123456")
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        adapter
            .start_native_harness(&launch, &pane, Some("123456"))
            .expect("started");
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        let prepared = calls
            .lines()
            .find(|line| line.contains("pane run"))
            .expect("pane prepared");
        assert!(prepared.contains("export FLOW_SOCKET='"));
        assert!(prepared.contains("/fixture/run/flow-next/flow/flow.sock'"));
        assert!(prepared.contains("export FLOW_ID=123456"));
        assert!(prepared.contains("FLOW_CLAUDE_ENV_READY_"));
        assert!(!prepared.contains("CLAUDE_CODE_ARTIFACT"));
        let start = fs::read_to_string(root.path().join("owned-command")).unwrap();
        assert!(start.contains(&format!("'--session-id' '{session}'")));
        let flow_id_calls = fs::read_to_string(root.path().join("flow-id-calls")).unwrap();
        assert!(flow_id_calls.starts_with(&format!(
            "claude --flows-root {} --parent-session {session}|",
            root.path().join("flows").display()
        )));
        assert_eq!(
            adapter
                .observe_native_binding(&launch, &pane, Some("123456"))
                .expect("bound as reserved")
                .flow_id,
            "123456"
        );
    }

    #[test]
    fn reserved_claude_binds_selected_session_without_herdr_session_and_refuses_mismatches() {
        use crate::herdr::reservation::{ChoosesNativeSession, ReservesFlowIdentity};
        let launch = launch(HarnessKind::Claude);
        let session = HerdrCli::default()
            .with_ordinary_socket(Path::new("/fixture/run/flow-next/flow/flow.sock"))
            .reserved_native_session_id(&launch)
            .expect("a reserved Claude launch chooses its session");
        let agent_name = launch.launch_agent_name();
        let (root, adapter) =
            fixture_herdr("claude", &session, &session.replace('-', ""), &agent_name);
        assert_eq!(
            adapter.reserve_flow_identity(&launch).unwrap().as_deref(),
            Some("123456")
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        adapter
            .start_native_harness(&launch, &pane, Some("123456"))
            .expect("started");
        fs::write(root.path().join("missing-agent-session"), "absent").unwrap();
        report_fixture_readiness(&adapter, &pane);
        let binding = adapter
            .observe_native_binding(&launch, &pane, Some("123456"))
            .expect("selected session binds without Herdr session");
        assert_eq!(binding.native_session_id, session);
        assert!(
            fs::read_to_string(root.path().join("owned-command"))
                .unwrap()
                .contains(&format!("'--session-id' '{session}'"))
        );
    }

    #[test]
    fn a_stale_managed_session_does_not_replace_the_flow_owned_session() {
        use crate::herdr::reservation::ChoosesNativeSession;
        let launch = launch(HarnessKind::Claude);
        let selected = HerdrCli::default()
            .with_ordinary_socket(Path::new("/fixture/run/flow-next/flow/flow.sock"))
            .reserved_native_session_id(&launch)
            .expect("selected session");
        let agent_name = launch.launch_agent_name();
        // Herdr's managed detection reports a stale session, while the
        // launch claim and readiness handshake name the selected one.
        let (root, adapter) = fixture_herdr(
            "claude",
            "12345678-1234-4abc-8def-123456789abc",
            &selected.replace('-', ""),
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        adapter
            .start_native_harness(&launch, &pane, Some("123456"))
            .expect("started");
        report_fixture_readiness(&adapter, &pane);
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        assert!(calls.contains("--session-id"));
        assert_eq!(
            adapter
                .observe_native_binding(&launch, &pane, Some("123456"))
                .expect("Flow-owned session binds")
                .native_session_id,
            selected
        );
    }

    #[test]
    fn stages_pane_native_claim_registration_and_one_ambiguous_prompt_write() {
        use crate::herdr::reservation::ChoosesNativeSession;
        use crate::title::TitlesFlow;
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        adapter
            .start_native_harness(&launch, &pane, Some("123456"))
            .expect("started without prompt");
        let calls_after_start =
            fs::read_to_string(root.path().join("calls")).expect("launch calls");
        let start_call = fs::read_to_string(root.path().join("owned-command"))
            .expect("Flow-owned start command");
        // The Remote Control name is the title, the same string as the
        // session name; no other name is given.
        let title = launch
            .launch_profile
            .native_title("123456")
            .expect("a title")
            .as_str()
            .to_owned();
        assert_eq!(title, "{ Field flow Secondary 123456 }");
        let session = adapter
            .reserved_native_session_id(&launch)
            .expect("a Claude launch chooses its session");
        let system_prompt_file = adapter
            .held_system_prompt_path(&launch.launch_profile.launch_request_id)
            .expect("the system prompt is held until the session binds");
        assert!(system_prompt_file.starts_with(&format!("/proc/{}/fd/", std::process::id())));
        assert_eq!(
            fs::read_to_string(root.path().join("system-prompt-read")).unwrap(),
            SYSTEM_TEXT,
            "the harness read the composed system prompt whole from the held file"
        );
        for pair in [
            format!("'--remote-control' {}", title.shell_quoted()),
            format!("'--name' {}", title.shell_quoted()),
            format!("'--system-prompt-file' '{}'", system_prompt_file),
            format!("'--session-id' '{session}'"),
            "'--model' 'model-current'".into(),
            "'--effort' 'high'".into(),
        ] {
            assert!(start_call.contains(&pair), "{pair}: {start_call}");
        }
        // The flag settings name the launch's own mode, which suppresses the
        // auto-mode default offer, and carry the harness hook on SessionStart,
        // every PostToolUse, UserPromptSubmit, Stop and SessionEnd; no
        // settings file is written.
        assert_eq!(start_call.matches("'--settings'").count(), 1);
        // Each flag once in the pane's argv: the installed wrapper execs
        // `.claude-wrapped --dangerously-skip-permissions "$@"`, so Flow's
        // own arguments never carry that flag.
        assert!(!start_call.contains(HerdrCli::CLAUDE_SKIP_PERMISSIONS_FLAG));
        assert!(!start_call.contains("launch-42"));
        assert!(
            !start_call.contains("--remote-control flow-"),
            "{start_call}"
        );
        assert!(!start_call.contains("composed body"));
        let pane_run = calls_after_start
            .lines()
            .position(|line| line.contains("pane run"))
            .expect("Claude environment preparation");
        let pane_wait = calls_after_start
            .lines()
            .position(|line| line.contains("pane wait-output"))
            .expect("Claude environment preparation receipt");
        let owned_start = calls_after_start
            .lines()
            .position(|line| line.contains("pane run w7:p1 '/fixture/claude'"))
            .expect("Claude start");
        assert!(pane_run < pane_wait && pane_wait < owned_start);
        let wait_call = calls_after_start
            .lines()
            .find(|line| line.contains("pane wait-output"))
            .unwrap();
        assert!(wait_call.contains("--source recent-unwrapped"));
        assert!(calls_after_start.contains("unset CLAUDE_CODE_CHILD_SESSION"));
        let create_call = calls_after_start
            .lines()
            .find(|line| line.contains("workspace create"))
            .expect("workspace create call");
        assert!(create_call.contains(&format!("--cwd {}", root.path().display())));
        assert!(!create_call.contains("/home/li/primary"));
        let binding = adapter
            .observe_native_binding(&launch, &pane, None)
            .expect("official native binding");
        assert_eq!(binding.flow_id, "123456");
        assert_eq!(
            adapter.held_system_prompt_path(&launch.launch_profile.launch_request_id),
            None,
            "the held system prompt is closed once the session has bound"
        );
        let flow_id_calls =
            fs::read_to_string(root.path().join("flow-id-calls")).expect("flow-id calls");
        assert!(flow_id_calls.contains(&format!(
            "claude --flows-root {} --parent-session {native_session}|",
            root.path().join("flows").display()
        )));
        let acknowledgement = RegistrationAcknowledgement {
            launch_request_id: binding.launch_request_id.clone(),
            flow_id: binding.flow_id.clone(),
            native_session_id: binding.native_session_id.clone(),
            herdr_pane_binding: binding.herdr_pane_binding.clone(),
        };
        let intent = adapter
            .accept_registration(&launch, &binding, &acknowledgement)
            .expect("registered intent");
        // The session named at start has already recorded its title.
        assert!(matches!(
            &intent.native_transcript_boundary,
            NativeTranscriptBoundary::Existing(_)
        ));
        assert_eq!(intent.prompt_sha256_vector, [PROMPT_HASH]);
        assert_eq!(intent.parts_submitted, 1);
        assert_eq!(
            adapter
                .submit_startup_part(&launch, &intent)
                .expect("single socket write"),
            PromptDeliveryResult::Ambiguous(intent)
        );
        // The whole message went once, as one `agent.prompt` request on
        // Herdr's socket; nothing was typed through an argument.
        let prompts = socket_prompts(&root);
        assert_eq!(prompts.len(), 1);
        assert_eq!(prompts[0]["method"], "agent.prompt");
        assert_eq!(prompts[0]["params"]["target"], pane.herdr_pane_id);
        assert_eq!(prompts[0]["params"]["text"], "composed body");
        let all_calls = fs::read_to_string(root.path().join("calls")).expect("all launch calls");
        assert!(!all_calls.contains("agent prompt"));
    }

    #[test]
    fn narrow_and_normal_claude_panes_require_unwrapped_marker_and_native_identity() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        for width in [54, 160] {
            let launch = launch(HarnessKind::Claude);
            let agent_name = launch.launch_agent_name();
            let (root, adapter) = fixture_herdr(
                "claude",
                native_session,
                "1234567812344abc8def123456789abc",
                &agent_name,
            );
            fs::write(root.path().join("pane-width"), width.to_string()).unwrap();
            let pane = adapter.create_launch_pane(&launch).unwrap();
            adapter
                .prepare_claude_pane_environment(&launch, &pane, None)
                .unwrap();
            fs::write(
                root.path().join("owned-command"),
                format!(
                    "'/fixture/claude' '--session-id' '{native_session}' '--model' 'model-current'"
                ),
            )
            .unwrap();
            let unwrapped = fs::read_to_string(root.path().join("marker-unwrapped")).unwrap();
            let visible = fs::read_to_string(root.path().join("marker-visible")).unwrap();
            assert!(unwrapped.trim().len() > 54);
            assert_eq!(
                visible.lines().any(|line| line == unwrapped.trim()),
                width == 160
            );
            let binding = adapter
                .observe_native_binding(&launch, &pane, None)
                .unwrap();
            assert_eq!(binding.native_session_id, native_session);
        }
        for (native, claim, reason) in [
            (
                "",
                "1234567812344abc8def123456789abc",
                "empty native identity",
            ),
            (
                native_session,
                "8765432112344abc8def123456789abc",
                "observed native identity",
            ),
        ] {
            let launch = launch(HarnessKind::Claude);
            let (root, adapter) =
                fixture_herdr("claude", native, claim, &launch.launch_agent_name());
            fs::write(root.path().join("pane-width"), "54").unwrap();
            let pane = adapter.create_launch_pane(&launch).unwrap();
            adapter
                .prepare_claude_pane_environment(&launch, &pane, None)
                .unwrap();
            fs::write(
                root.path().join("owned-command"),
                format!("'/fixture/claude' '--session-id' '{native}' '--model' 'model-current'"),
            )
            .unwrap();
            let refusal = adapter
                .observe_native_binding(&launch, &pane, None)
                .unwrap_err();
            assert!(refusal.contains(reason), "expected {reason}: {refusal}");
        }
    }

    #[test]
    fn missing_official_identity_and_mismatched_claim_fail_closed() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Codex);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "codex",
            native_session,
            "ffffffffffffffffffffffffffffffff",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        assert!(
            adapter
                .observe_native_binding(&launch, &pane, None)
                .unwrap_err()
                .contains("observed native identity")
        );
        let flow_id_calls =
            fs::read_to_string(root.path().join("flow-id-calls")).expect("flow-id calls");
        assert!(flow_id_calls.contains(&format!(
            "codex --flows-root {}|{native_session}",
            root.path().join("flows").display()
        )));

        let (_missing_root, missing_adapter) =
            fixture_herdr("codex", "", "ffffffffffffffffffffffffffffffff", &agent_name);
        let missing_pane = missing_adapter
            .create_launch_pane(&launch)
            .expect("created missing-identity pane");
        assert!(
            missing_adapter
                .observe_native_binding(&launch, &missing_pane, None)
                .unwrap_err()
                .contains("empty native identity")
        );
    }

    #[test]
    fn unsupported_workspace_roots_are_rejected_before_herdr_creation() {
        let launch = launch(HarnessKind::Codex);
        let agent_name = launch.launch_agent_name();
        let (root, mut adapter) = fixture_herdr(
            "codex",
            "12345678-1234-4abc-8def-123456789abc",
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        adapter.flows_root = root.path().join("not-flows");
        assert!(
            adapter
                .create_launch_pane(&launch)
                .unwrap_err()
                .contains("directory named flows")
        );
        assert!(!root.path().join("calls").exists());
    }

    /// The thread's rollout exists from its naming, before the TUI
    /// attached: the receipt is read from the thread's start, so the first
    /// turn Submit gives it after the registration and its answer are
    /// observed.
    #[test]
    fn a_codex_first_turn_is_read_from_the_thread_start() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Codex);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "codex",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        let transcript = root
            .path()
            .join("native-transcripts/codex")
            .join(format!("rollout-{native_session}.jsonl"));
        let meta = serde_json::json!({"type":"session_meta","payload":{"id":native_session}});
        let context = serde_json::json!({"type":"turn_context","payload":{
            "turn_id":"turn-first","model":"model-current","effort":"high"}});
        let input = serde_json::json!({"type":"event_msg","payload":{
            "thread_id":native_session,"turn_id":"turn-first",
            "item":{"type":"UserMessage","content":[{"type":"text","text":launch.first_prompt_payload.first_prompt_body}]}}});
        fs::write(&transcript, format!("{meta}\n")).expect("named thread");
        let intent = registered_intent(&adapter, &launch, pane, native_session);
        let NativeTranscriptBoundary::Existing(cursor) = &intent.native_transcript_boundary else {
            panic!("the thread's transcript exists");
        };
        assert_eq!(cursor.transcript_byte_offset, 0);
        use std::io::Write;
        let mut append = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .expect("append transcript");
        write!(append, "{context}\n{input}\n").expect("the first turn");
        // The part alone is not the receipt: the flow has not answered.
        assert_eq!(
            adapter.observe_native_target_receipt(&intent).unwrap(),
            StartupProgress::Waiting(intent.clone())
        );
        let answer = serde_json::json!({"type":"response_item","payload":{
            "type":"message","role":"assistant","content":[{"type":"output_text","text":"Beginning."}]}});
        writeln!(append, "{answer}").expect("append the first answer");
        let StartupProgress::Received(NativeTargetReceipt { native_turn_id, .. }) = adapter
            .observe_native_target_receipt(&intent)
            .expect("observed transcript receipt")
        else {
            panic!("receipt remained ambiguous");
        };
        assert_eq!(native_turn_id, "turn-first");
    }

    /// A request that carried a block the launch left out refuses the
    /// receipt: here the AGENTS.md body and the skill listing, as Codex
    /// puts them before the first turn's input.
    #[test]
    fn a_codex_request_carrying_a_left_out_block_is_refused() {
        use signal_flow::LeaveOut;
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let agents = serde_json::json!({"type":"response_item","payload":{"type":"message",
            "role":"user","content":[{"type":"input_text",
            "text":"# AGENTS.md instructions for /primary\n\n<INSTRUCTIONS>\nbody"}]}});
        let skills = serde_json::json!({"type":"response_item","payload":{"type":"message",
            "role":"developer","content":[{"type":"input_text",
            "text":"<skills_instructions>\n## Skills\nA skill is"}]}});
        let role = serde_json::json!({"type":"response_item","payload":{"type":"message",
            "role":"developer","content":[{"type":"input_text",
            "text":"<multi_agent_role>You are `/root`, the primary agent"}]}});
        let mode = serde_json::json!({"type":"response_item","payload":{"type":"message",
            "role":"developer","content":[{"type":"input_text",
            "text":"<multi_agent_mode>Any earlier instruction enabling proactive"}]}});
        for (block, leave_out, kept) in [
            (agents, LeaveOut::CodexAgentsMd, LeaveOut::CodexSkillListing),
            (skills, LeaveOut::CodexSkillListing, LeaveOut::CodexAgentsMd),
            (
                role,
                LeaveOut::CodexDeveloperInstructions,
                LeaveOut::CodexAgentsMd,
            ),
            (
                mode,
                LeaveOut::CodexDeveloperInstructions,
                LeaveOut::CodexAgentsMd,
            ),
        ] {
            let mut launch = launch(HarnessKind::Codex);
            let agent_name = launch.launch_agent_name();
            let (root, adapter) = fixture_herdr(
                "codex",
                native_session,
                "1234567812344abc8def123456789abc",
                &agent_name,
            );
            let pane = adapter.create_launch_pane(&launch).expect("created pane");
            let transcript = root
                .path()
                .join("native-transcripts/codex")
                .join(format!("rollout-{native_session}.jsonl"));
            let context = serde_json::json!({"type":"turn_context","payload":{
                "turn_id":"turn-first","model":"model-current","effort":"high"}});
            let meta = serde_json::json!({"type":"session_meta","payload":{"id":native_session}});
            fs::write(&transcript, format!("{meta}\n{block}\n{context}\n")).expect("first request");
            // Left in, the block is the launch's own choice.
            launch.leave_outs = vec![kept];
            let intent = registered_intent(&adapter, &launch, pane.clone(), native_session);
            assert_eq!(
                adapter.observe_native_target_receipt(&intent).unwrap(),
                StartupProgress::Waiting(intent.clone())
            );
            launch.leave_outs = vec![leave_out.clone()];
            let intent = registered_intent(&adapter, &launch, pane, native_session);
            let refusal = adapter.observe_native_target_receipt(&intent).unwrap_err();
            assert_eq!(
                refusal,
                super::TranscriptRefusal::Contradicted(NativeLaunchFailure::LeaveOutShown),
                "{leave_out:?}"
            );
        }
    }

    /// The Codex TUI is started attached to the thread Flow opened, the
    /// subcommand after every switch, and binds only to that thread.
    #[test]
    fn a_codex_tui_attaches_to_the_opened_thread_and_binds_only_to_it() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Codex);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "codex",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        adapter
            .attach_codex_harness(&launch, &pane, native_session)
            .expect("attached Codex client");
        let calls = fs::read_to_string(root.path().join("calls")).expect("launch calls");
        let start = calls
            .lines()
            .find(|line| line.contains("agent start"))
            .expect("agent start call");
        assert!(start.contains("-- --remote unix:///tmp/stable-codex.sock --model model-current"));
        assert!(start.ends_with(&format!("resume {native_session}")));
        assert_eq!(start.matches(" resume ").count(), 1);
        let binding = adapter
            .observe_attached_codex_binding(&launch, &pane, native_session)
            .expect("the opened thread binds");
        assert_eq!(binding.native_session_id, native_session);
        assert!(
            adapter
                .observe_attached_codex_binding(
                    &launch,
                    &pane,
                    "87654321-4321-4cba-8fed-cba987654321"
                )
                .unwrap_err()
                .contains("another thread than Flow opened")
        );
        let claude = super::tests::launch(HarnessKind::Claude);
        assert!(
            adapter
                .attach_codex_harness(&claude, &pane, native_session)
                .is_err()
        );
    }

    /// Herdr reports no agent session for a TUI that remote-resumed a
    /// thread: the pane binds to the thread Flow opened once Herdr
    /// recognizes its Codex agent, exact and ready, and never to a pane
    /// Herdr does not recognize as Codex.
    #[test]
    fn a_remote_resumed_codex_tui_binds_to_the_opened_thread_without_a_herdr_session() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Codex);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "codex",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        fs::write(root.path().join("never-agent-session"), "absent").unwrap();
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        adapter
            .attach_codex_harness(&launch, &pane, native_session)
            .expect("attached Codex client");
        let binding = adapter
            .observe_attached_codex_binding(&launch, &pane, native_session)
            .expect("the opened thread binds without a Herdr session");
        assert_eq!(binding.native_session_id, native_session);
        assert_eq!(binding.flow_id, "123456");
        assert_eq!(binding.harness_kind, HarnessKind::Codex);
        // Without Herdr's own report, the attached thread is still required.
        assert!(
            adapter
                .observe_attached_codex_binding(&launch, &pane, "")
                .is_err()
        );
        // A pane Herdr recognizes as another harness does not bind.
        fs::write(root.path().join("reported-harness"), "claude").unwrap();
        assert!(
            adapter
                .observe_attached_codex_binding(&launch, &pane, native_session)
                .unwrap_err()
                .contains("does not match the created native pane")
        );
    }

    /// The Codex target is the thread Flow opened and the recognized pane:
    /// registration and the receipt read need no Herdr session.
    #[test]
    fn a_codex_registration_and_receipt_need_no_herdr_session() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Codex);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "codex",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        fs::write(root.path().join("never-agent-session"), "absent").unwrap();
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        let transcript = root
            .path()
            .join("native-transcripts/codex")
            .join(format!("rollout-{native_session}.jsonl"));
        fs::write(
            &transcript,
            format!("{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{native_session}\"}}}}\n"),
        )
        .unwrap();
        let intent = registered_intent(&adapter, &launch, pane, native_session);
        assert!(matches!(
            adapter.observe_native_target_receipt(&intent).unwrap(),
            StartupProgress::Waiting(_)
        ));
    }

    /// A session Herdr does report must be the thread Flow opened; Claude
    /// must report one.
    #[test]
    fn a_reported_session_must_be_the_opened_thread() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let other = "87654321-4321-4cba-8fed-cba987654321";
        let launch = launch(HarnessKind::Codex);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "codex",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        assert!(
            adapter
                .verify_native_target(&pane, native_session, &HarnessKind::Codex)
                .is_ok()
        );
        assert_eq!(
            adapter
                .verify_native_target(&pane, other, &HarnessKind::Codex)
                .unwrap_err(),
            NativeTargetRefusal::IdentityInexact
        );
        fs::write(root.path().join("never-agent-session"), "absent").unwrap();
        assert!(
            adapter
                .verify_native_target(&pane, other, &HarnessKind::Codex)
                .is_ok()
        );
    }

    /// The transcript is the authority for the Codex thread when Herdr
    /// reports none: its first record names exactly the thread Flow opened.
    /// Every registration stage reaches the wire as its own variant; the
    /// adapter text a stage keeps for the log does not.
    #[test]
    fn every_registration_stage_is_carried_on_the_wire() {
        use super::RegistrationRefusal as Stage;
        use signal_flow::RegistrationRefusal as Wire;
        for (stage, wire) in [
            (Stage::PaneBindingForeign, Wire::PaneBindingForeign),
            (
                Stage::AcknowledgementMismatch,
                Wire::AcknowledgementMismatch,
            ),
            (Stage::PromptNotCanonical, Wire::PromptNotCanonical),
            (
                Stage::NativeTarget(NativeTargetRefusal::HerdrUnreadable(
                    "secret=/private/socket".into(),
                )),
                Wire::NativeTarget(signal_flow::NativeTargetRefusal::HerdrUnreadable),
            ),
            (
                Stage::NativeTarget(NativeTargetRefusal::PaneMismatch),
                Wire::NativeTarget(signal_flow::NativeTargetRefusal::PaneMismatch),
            ),
            (
                Stage::NativeTarget(NativeTargetRefusal::NoNativeSession),
                Wire::NativeTarget(signal_flow::NativeTargetRefusal::NoNativeSession),
            ),
            (
                Stage::NativeTarget(NativeTargetRefusal::IdentityInexact),
                Wire::NativeTarget(signal_flow::NativeTargetRefusal::IdentityInexact),
            ),
            (
                Stage::TranscriptBoundary("secret=/private/root".into()),
                Wire::TranscriptBoundary,
            ),
            (Stage::ThreadNotNamed, Wire::ThreadNotNamed),
        ] {
            assert_eq!(Wire::from(&stage), wire);
            assert!(!format!("{wire:?}").contains("secret="));
        }
    }

    #[test]
    fn a_codex_transcript_must_name_the_opened_thread() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Codex);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "codex",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        fs::write(root.path().join("never-agent-session"), "absent").unwrap();
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        let transcript = root
            .path()
            .join("native-transcripts/codex")
            .join(format!("rollout-{native_session}.jsonl"));
        // A rollout whose session meta names another thread is refused at
        // registration, at the stage that names it.
        fs::write(
            &transcript,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"another-thread\"}}\n",
        )
        .unwrap();
        assert_eq!(
            registration_of(&adapter, &launch, pane.clone(), native_session).unwrap_err(),
            super::RegistrationRefusal::ThreadNotNamed
        );
        // One that names no thread first is refused as well.
        fs::write(&transcript, "{\"type\":\"turn_context\"}\n").unwrap();
        assert_eq!(
            registration_of(&adapter, &launch, pane.clone(), native_session).unwrap_err(),
            super::RegistrationRefusal::ThreadNotNamed
        );
        // A transcript that appears after registration is read the same way.
        fs::remove_file(&transcript).unwrap();
        let intent = registered_intent(&adapter, &launch, pane, native_session);
        fs::write(
            &transcript,
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"another-thread\"}}\n",
        )
        .unwrap();
        assert!(
            adapter.observe_native_target_receipt(&intent).unwrap_err()
                == super::TranscriptRefusal::Contradicted(NativeLaunchFailure::ThreadNotNamed)
        );
    }

    #[test]
    fn codex_receipt_rejects_a_wrong_first_text() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Codex);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "codex",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        let transcript = root
            .path()
            .join("native-transcripts/codex")
            .join(format!("rollout-{native_session}.jsonl"));
        fs::write(
            &transcript,
            format!("{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{native_session}\"}}}}\n"),
        )
        .expect("initial transcript");
        let intent = registered_intent(&adapter, &launch, pane, native_session);
        let rows = [
            serde_json::json!({"type":"turn_context","payload":{
                "turn_id":"turn-wrong","model":"model-current","effort":"high"}}),
            serde_json::json!({"type":"event_msg","payload":{
                "thread_id":native_session,"turn_id":"turn-wrong",
                "item":{"type":"UserMessage","content":[{"type":"text","text":"a body the intent never hashed"}]}}}),
        ];
        use std::io::Write;
        let mut append = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .expect("append transcript");
        for row in rows {
            writeln!(append, "{row}").expect("append receipt row");
        }
        assert!(
            adapter.observe_native_target_receipt(&intent).unwrap_err()
                == super::TranscriptRefusal::Contradicted(NativeLaunchFailure::PartDiffers)
        );
    }

    #[test]
    fn an_absent_receipt_is_never_invented_and_the_first_answer_is_observed() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        let intent = registered_intent(&adapter, &launch, pane, native_session);
        assert!(matches!(
            &intent.native_transcript_boundary,
            NativeTranscriptBoundary::Absent(_)
        ));
        let untrusted_transcript = root.path().join("caller-selected.jsonl");
        let untrusted_row = serde_json::json!({"type":"assistant","sessionId":native_session,
            "uuid":"untrusted-turn","message":{"content":[{"type":"text","text":"work"}]}});
        fs::write(&untrusted_transcript, format!("{}\n", untrusted_row))
            .expect("untrusted transcript");
        assert_eq!(
            adapter
                .observe_native_target_receipt(&intent)
                .expect("caller-selected transcript is ignored"),
            StartupProgress::Waiting(intent.clone())
        );
        let transcript = root
            .path()
            .join("native-transcripts/claude")
            .join(format!("{native_session}.jsonl"));
        let first_input = serde_json::json!({"type":"user","sessionId":native_session,
            "message":{"role":"user","content":launch.first_prompt_payload.first_prompt_body}});
        fs::write(&transcript, format!("{first_input}\n")).expect("empty receipt transcript");
        let StartupProgress::Waiting(adopted_intent) = adapter
            .observe_native_target_receipt(&intent)
            .expect("absence is ambiguous and adopts the new transcript")
        else {
            panic!("receipt was invented");
        };
        assert!(matches!(
            &adopted_intent.native_transcript_boundary,
            NativeTranscriptBoundary::Existing(_)
        ));
        let row = serde_json::json!({"type":"assistant","sessionId":native_session,
            "uuid":"turn-claude","effort":"high","message":{"model":"model-current",
            "content":[{"type":"text","text":"Beginning the brief."}]}});
        use std::io::Write;
        let mut append = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .expect("append the first answer");
        writeln!(append, "{row}").expect("first answer");
        let StartupProgress::Received(observed) = adapter
            .observe_native_target_receipt(&adopted_intent)
            .expect("the first answer is the receipt")
        else {
            panic!("the first answer after the authenticated turn is the receipt");
        };
        assert_eq!(observed.native_turn_id, "turn-claude");
        assert_eq!(
            observed.receipt_sha256,
            format!(
                "{:x}",
                <sha2::Sha256 as sha2::Digest>::digest(format!("{row}\n").as_bytes())
            )
        );
    }

    /// e167d8 sandbox: Claude Haiku 4.5 takes no effort, and Claude Code
    /// 2.1.280 records its turns with no `effort` and `perTurnEffort: null`.
    /// The seat's first answer must still be read; a recorded effort that
    /// differs is refused.
    #[test]
    fn claude_receipt_of_a_model_without_effort_is_observed() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        let intent = registered_intent(&adapter, &launch, pane, native_session);
        let transcript = root
            .path()
            .join("native-transcripts/claude")
            .join(format!("{native_session}.jsonl"));
        let first_input = serde_json::json!({"type":"user","sessionId":native_session,
            "message":{"role":"user","content":launch.first_prompt_payload.first_prompt_body}});
        let receipt = |effort: serde_json::Value| {
            let mut row = serde_json::json!({"type":"assistant","sessionId":native_session,
                "session_id":native_session,"uuid":"turn-claude","perTurnEffort":null,
                "message":{"model":"model-current",
                "content":[{"type":"text","text":"Beginning the brief."}]}});
            if !effort.is_null() {
                row["effort"] = effort.clone();
                row["perTurnEffort"] = effort;
            }
            row
        };

        fs::write(
            &transcript,
            format!("{first_input}\n{}\n", receipt(serde_json::Value::Null)),
        )
        .expect("receipt without effort");
        let StartupProgress::Received(observed) = adapter
            .observe_native_target_receipt(&intent)
            .expect("a turn with no effort recorded is read")
        else {
            panic!("a model without effort never reaches Observed");
        };
        assert_eq!(observed.native_turn_id, "turn-claude");
        assert_eq!(observed.effort, Effort::from("high"));

        fs::write(
            &transcript,
            format!("{first_input}\n{}\n", receipt(serde_json::json!("low"))),
        )
        .expect("receipt with another effort");
        assert!(
            adapter.observe_native_target_receipt(&intent).unwrap_err()
                == super::TranscriptRefusal::Contradicted(
                    NativeLaunchFailure::ModelOrEffortDiffers
                )
        );
    }

    /// The receipt compares the model the transcript records with the model
    /// the launch named, string for string: the same model at the same
    /// effort is received; another model at the same effort, or the same
    /// model at another effort, is refused.
    #[test]
    fn a_claude_receipt_is_received_only_for_the_exact_model_and_effort() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        let intent = registered_intent(&adapter, &launch, pane, native_session);
        let transcript = root
            .path()
            .join("native-transcripts/claude")
            .join(format!("{native_session}.jsonl"));
        let first_input = serde_json::json!({"type":"user","sessionId":native_session,
            "message":{"role":"user","content":launch.first_prompt_payload.first_prompt_body}});
        let receipt = |model: &str, effort: &str| {
            serde_json::json!({"type":"assistant","sessionId":native_session,
                "session_id":native_session,"uuid":"turn-claude",
                "effort":effort,"perTurnEffort":effort,
                "message":{"model":model,
                "content":[{"type":"text","text":"Beginning the brief."}]}})
        };
        let observed = |model: &str, effort: &str| {
            fs::write(
                &transcript,
                format!("{first_input}\n{}\n", receipt(model, effort)),
            )
            .expect("receipt written");
            adapter.observe_native_target_receipt(&intent)
        };
        assert_eq!(intent.model_name, ModelName::from("model-current"));
        assert!(matches!(
            observed("model-current", "high"),
            Ok(StartupProgress::Received(_))
        ));
        for (model, effort) in [("model-other", "high"), ("model-current", "low")] {
            assert!(
                observed(model, effort).unwrap_err()
                    == super::TranscriptRefusal::Contradicted(
                        NativeLaunchFailure::ModelOrEffortDiffers
                    ),
                "{model} {effort}"
            );
        }
    }

    #[test]
    fn registered_harness_change_with_same_native_id_is_rejected() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        let intent = registered_intent(&adapter, &launch, pane, native_session);
        fs::write(root.path().join("reported-harness"), "codex").expect("changed harness");
        assert!(
            adapter
                .observe_native_target_receipt(&intent)
                .unwrap_err()
                .to_string()
                .contains("native identity is no longer exact")
        );
    }

    /// A Claude cursor sits past the transcript's end when the first part
    /// is typed (a Codex thread is read from its start), and the transcript behind it must stay as it was.
    #[test]
    fn persisted_cursor_rejects_truncation_prefix_change_and_replacement() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        let transcript = root
            .path()
            .join("native-transcripts/claude")
            .join(format!("{native_session}.jsonl"));
        fs::write(&transcript, "{\"type\":\"user\"}\n").expect("initial transcript");
        let intent = registered_intent(&adapter, &launch, pane, native_session);

        fs::write(&transcript, "").expect("truncate transcript");
        assert!(
            adapter
                .observe_native_target_receipt(&intent)
                .unwrap_err()
                .to_string()
                .contains("truncated")
        );

        fs::write(&transcript, "{\"type\":\"xxxx\"}\n").expect("rewrite transcript prefix");
        assert!(
            adapter
                .observe_native_target_receipt(&intent)
                .unwrap_err()
                .to_string()
                .contains("prefix changed")
        );

        let replacement = root.path().join("replacement.jsonl");
        fs::write(&replacement, "{\"type\":\"user\"}\n").expect("replacement transcript");
        fs::rename(&replacement, &transcript).expect("replace transcript inode");
        assert!(
            adapter
                .observe_native_target_receipt(&intent)
                .unwrap_err()
                .to_string()
                .contains("file was replaced")
        );
    }

    /// Claude Code 2.1.294 keeps a long typed message inside its native
    /// paste element; the element is taken off before the part's hash is read.
    #[test]
    fn a_pasted_part_is_read_through_its_wrapper() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        let transcript = root
            .path()
            .join("native-transcripts/claude")
            .join(format!("{native_session}.jsonl"));
        fs::write(&transcript, "{\"type\":\"session_meta\"}\n").unwrap();
        let intent = registered_intent(&adapter, &launch, pane, native_session);
        let pasted = serde_json::json!({"type":"user","sessionId":native_session,
            "message":{"role":"user","content":format!(
                "\n\n<pasted_content id=\"04ee\">\n{}\n</pasted_content id=\"04ee\">\n",
                launch.first_prompt_payload.first_prompt_body)}});
        let answer = serde_json::json!({"type":"assistant","sessionId":native_session,
            "uuid":"turn-pasted","effort":"high","message":{"model":"model-current",
            "content":[{"type":"text","text":"Beginning."}]}});
        let mut append = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        writeln!(append, "{pasted}\n{answer}").unwrap();
        let StartupProgress::Received(receipt) =
            adapter.observe_native_target_receipt(&intent).unwrap()
        else {
            panic!("the pasted part and its answer are the receipt");
        };
        assert_eq!(receipt.native_turn_id, "turn-pasted");
        assert_eq!(receipt.prompt_sha256_vector, [PROMPT_HASH]);
    }

    /// Claude Code 2.1.294's paste escape, as its own expression
    /// `lI(["pasted_content"])` replaced with `<\` gives it.
    #[test]
    fn the_paste_escape_is_claude_codes() {
        let cases = [
            (
                "quote <pasted_content id=\"04ee\"> here",
                "quote <\\pasted_content id=\"04ee\"> here",
            ),
            (
                "</pasted_content id=\"04ee\">",
                "<\\/pasted_content id=\"04ee\">",
            ),
            ("<\\pasted_content", "<\\pasted_content"),
            ("<Pasted-Content", "<\\Pasted-Content"),
            ("< pasted_content", "<\\ pasted_content"),
            ("\u{ff1c}pasted_content", "<\\pasted_content"),
            ("<pasted_contents", "<pasted_contents"),
            ("<pasted_content_", "<pasted_content_"),
            ("<pas\u{200b}ted_content", "<\\pas\u{200b}ted_content"),
            ("<pasted", "<pasted"),
            ("<<pasted_content>", "<<\\pasted_content>"),
            ("<\u{ff50}asted_content", "<\\\u{ff50}asted_content"),
            ("a < b", "a < b"),
        ];
        for (text, escaped) in cases {
            assert_eq!(text.claude_paste_escaped(), escaped, "{text:?}");
        }
    }

    /// Only the element rebuilt from the part, byte for byte, is read.
    #[test]
    fn only_a_paste_element_rebuilt_exactly_from_the_part_is_read() {
        let part = "Address.{ A }\nOrder: STARTUP_READY";
        let hash = digest(part);
        let element = |opening: &str, closing: &str| {
            format!(
                "\n\n<pasted_content id=\"{opening}\">\n{part}\n</pasted_content id=\"{closing}\">\n"
            )
        };
        assert_eq!(part.claude_paste_element("04ee"), element("04ee", "04ee"));
        assert_eq!(
            element("04ee", "04ee").claude_pasted_part(&hash),
            Some(part.to_owned())
        );
        let refused = [
            format!("x{}", element("04ee", "04ee")),
            format!("{}x", element("04ee", "04ee")),
            format!("{} ", element("04ee", "04ee")),
            element("04ee", "04ef"),
            element("", ""),
            element("04EE", "04EE"),
            format!("{}{}", element("04ee", "04ee"), element("05ff", "05ff")),
            format!("<pasted_content id=\"04ee\">\n{part}\n</pasted_content>"),
            format!("\n\n<pasted_content id=\"04ee\">\n{part}\n</pasted_content>\n"),
            format!("\n\n<pasted_content>\n{part}\n</pasted_content>\n"),
            format!(
                "\n\n<pasted_content id=\"04ee\" x=\"1\">\n{part}\n</pasted_content id=\"04ee\" x=\"1\">\n"
            ),
        ];
        for text in &refused {
            assert_eq!(text.claude_pasted_part(&hash), None, "{text:?}");
        }
        assert_eq!(part.claude_pasted_part(&hash), None);
    }

    /// The element Claude Code 2.1.294 records for a part whose escaped
    /// text is `escaped`, pasted under `identifier`.
    fn pasted(identifier: &str, escaped: &str) -> String {
        let newline = if escaped.ends_with('\n') { "" } else { "\n" };
        format!(
            "\n\n<pasted_content id=\"{identifier}\">\n{escaped}{newline}</pasted_content id=\"{identifier}\">\n"
        )
    }

    /// Reads `rows` as the Claude user texts of a launch whose parts are
    /// `parts`, each row answered; true when the answer after the last row
    /// is the flow's first answer.
    fn claude_rows_read(parts: &[&str], rows: &[String]) -> Result<bool, NativeLaunchFailure> {
        use crate::generated::starter::{ReadsStartupRecord, StartupReading};
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let (_root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &launch.launch_agent_name(),
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        let mut intent = registered_intent(&adapter, &launch, pane, native_session);
        intent.prompt_sha256_vector = parts.iter().copied().map(digest).collect();
        intent.parts_submitted = i64::try_from(parts.len()).unwrap();
        let mut reading = StartupReading {
            seen_parts: 0,
            last_part_answered: false,
            native_turn_option: None,
            part_turn_option: None,
        };
        let mut received = false;
        for (index, row) in rows.iter().enumerate() {
            let user = serde_json::json!({"type":"user","sessionId":native_session,
                "message":{"role":"user","content":row}});
            reading.read_record(&user, &intent)?;
            let answer = serde_json::json!({"type":"assistant","sessionId":native_session,
                "uuid":format!("turn-{index}"),"effort":"high","message":{
                "model":"model-current","content":[{"type":"text","text":"Waiting."}]}});
            received = reading.read_record(&answer, &intent)?.is_some();
        }
        Ok(received)
    }

    #[test]
    fn a_claude_paste_of_a_part_ending_in_a_newline_is_observed() {
        assert_eq!(
            claude_rows_read(&["X\n"], &[pasted("04ee", "X\n")]),
            Ok(true)
        );
    }

    #[test]
    fn a_claude_paste_holding_one_newline_more_than_the_part_differs() {
        assert_eq!(
            claude_rows_read(&["X\n"], &[pasted("04ee", "X\n")]),
            Ok(true)
        );
        let row = "\n\n<pasted_content id=\"04ee\">\nX\n\n</pasted_content id=\"04ee\">\n";
        assert_eq!(
            claude_rows_read(&["X"], &[row.to_owned()]),
            Err(NativeLaunchFailure::PartDiffers)
        );
    }

    #[test]
    fn a_claude_paste_holding_one_newline_fewer_than_the_part_differs() {
        assert_eq!(
            claude_rows_read(&["X\n\n"], &[pasted("04ee", "X\n\n")]),
            Ok(true)
        );
        let row = "\n\n<pasted_content id=\"04ee\">\nX\n</pasted_content id=\"04ee\">\n";
        assert_eq!(
            claude_rows_read(&["X\n\n"], &[row.to_owned()]),
            Err(NativeLaunchFailure::PartDiffers)
        );
    }

    #[test]
    fn a_claude_paste_id_other_than_four_lowercase_hex_digits_differs() {
        assert_eq!(
            claude_rows_read(&["X\n"], &[pasted("04ee", "X\n")]),
            Ok(true)
        );
        for identifier in ["04eG", "04ee5", "04EE", "04e", ""] {
            assert_eq!(
                claude_rows_read(&["X"], &[pasted(identifier, "X")]),
                Err(NativeLaunchFailure::PartDiffers),
                "{identifier:?}"
            );
        }
    }

    #[test]
    fn an_intermediate_claude_part_ending_in_a_newline_is_observed() {
        let parts = ["first module\n", "second module\n\n", "the brief"];
        let rows = [
            pasted("0a1b", parts[0]),
            pasted("c2d3", parts[1]),
            pasted("e4f5", parts[2]),
        ];
        assert_eq!(claude_rows_read(&parts, &rows), Ok(true));
    }

    #[test]
    fn a_claude_part_quoting_a_paste_tag_is_read_under_the_native_escape() {
        let part = "see <pasted_content id=\"04ee\">\nand </pasted_content id=\"04ee\">\n";
        let escaped = "see <\\pasted_content id=\"04ee\">\nand <\\/pasted_content id=\"04ee\">\n";
        assert_eq!(
            claude_rows_read(&[part], &[pasted("04ee", escaped)]),
            Ok(true)
        );
        // A tag the part already escaped is left as it stands.
        let quoted = "keep <\\pasted_content and <pasted_content\n";
        let quoted_escaped = "keep <\\pasted_content and <\\pasted_content\n";
        assert_eq!(
            claude_rows_read(&[quoted], &[pasted("04ee", quoted_escaped)]),
            Ok(true)
        );
        let wrong = [
            pasted("04ee", part),
            pasted(
                "04ee",
                "see <\\pasted_content id=\"04ee\">\nand </pasted_content id=\"04ee\">\n",
            ),
            pasted("04ee", &escaped.replace("<\\", "<\\\\")),
        ];
        for row in wrong {
            assert_eq!(
                claude_rows_read(&[part], std::slice::from_ref(&row)),
                Err(NativeLaunchFailure::PartDiffers),
                "{row:?}"
            );
        }
    }

    #[test]
    fn claude_parts_reordered_or_missing_differ() {
        let parts = ["A\n", "B\n", "C\n"];
        let row = |index: usize| pasted("04ee", parts[index]);
        assert_eq!(
            claude_rows_read(&parts, &[row(0), row(1), row(2)]),
            Ok(true)
        );
        assert_eq!(
            claude_rows_read(&parts, &[row(1), row(0), row(2)]),
            Err(NativeLaunchFailure::PartDiffers)
        );
        assert_eq!(
            claude_rows_read(&parts, &[row(0), row(2)]),
            Err(NativeLaunchFailure::PartDiffers)
        );
        assert_eq!(
            claude_rows_read(&parts, &[row(0), row(0), row(1), row(2)]),
            Err(NativeLaunchFailure::PartDiffers)
        );
    }

    /// A paste element holding other bytes than the part is not the part.
    #[test]
    fn a_pasted_element_with_other_content_is_not_the_part() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        let transcript = root
            .path()
            .join("native-transcripts/claude")
            .join(format!("{native_session}.jsonl"));
        fs::write(&transcript, "{\"type\":\"session_meta\"}\n").unwrap();
        let intent = registered_intent(&adapter, &launch, pane, native_session);
        let pasted = serde_json::json!({"type":"user","sessionId":native_session,
            "message":{"role":"user","content":format!(
                "\n\n<pasted_content id=\"04ee\">\n{} \n</pasted_content id=\"04ee\">\n",
                launch.first_prompt_payload.first_prompt_body)}});
        let mut append = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        writeln!(append, "{pasted}").unwrap();
        assert!(adapter.observe_native_target_receipt(&intent).is_err());
    }

    /// A message sent in two parts: the first part answered is not the
    /// receipt but the cue for the next; only the last part and the answer
    /// after it are the receipt, and a part out of order refuses.
    #[test]
    fn a_two_part_claude_message_is_received_part_by_part_in_order() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = two_part_launch(HarnessKind::Claude);
        assert!(
            HarnessKind::Claude.fits_one_message(&launch.first_prompt_payload.first_prompt_body)
        );
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        let transcript = root
            .path()
            .join("native-transcripts/claude")
            .join(format!("{native_session}.jsonl"));
        fs::write(&transcript, "{\"type\":\"session_meta\"}\n").unwrap();
        let intent = registered_intent(&adapter, &launch, pane, native_session);
        let parts = &launch.first_prompt_payload.prompt_part_vector;
        assert_eq!(intent.prompt_sha256_vector.len(), 2);
        assert_eq!(intent.parts_submitted, 1);
        let user = |text: &str| {
            serde_json::json!({"type":"user","sessionId":native_session,
                "message":{"role":"user","content":text}})
        };
        let answer = |uuid: &str| {
            serde_json::json!({"type":"assistant","sessionId":native_session,"uuid":uuid,
                "effort":"high","message":{"model":"model-current",
                "content":[{"type":"text","text":"Waiting."}]}})
        };
        let mut append = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        writeln!(append, "{}", user(&parts[0].prompt_part_text)).unwrap();
        assert_eq!(
            adapter.observe_native_target_receipt(&intent).unwrap(),
            StartupProgress::Waiting(intent.clone()),
            "the first part not yet answered"
        );
        writeln!(append, "{}", answer("turn-wait")).unwrap();
        assert_eq!(
            adapter.observe_native_target_receipt(&intent).unwrap(),
            StartupProgress::PartAnswered(intent.clone()),
            "the first part answered cues the second"
        );
        let mut next = intent.clone();
        next.parts_submitted = 2;
        assert_eq!(
            adapter.observe_native_target_receipt(&next).unwrap(),
            StartupProgress::Waiting(next.clone())
        );
        writeln!(append, "{}", user(&parts[1].prompt_part_text)).unwrap();
        writeln!(append, "{}", answer("turn-brief")).unwrap();
        let StartupProgress::Received(receipt) =
            adapter.observe_native_target_receipt(&next).unwrap()
        else {
            panic!("every part, then the answer after the last, is the receipt");
        };
        assert_eq!(receipt.native_turn_id, "turn-brief");
        assert_eq!(receipt.prompt_sha256_vector, next.prompt_sha256_vector);

        // The second part before the first is not the message sent.
        fs::write(
            &transcript,
            format!(
                "{{\"type\":\"session_meta\"}}\n{}\n",
                user(&parts[1].prompt_part_text)
            ),
        )
        .unwrap();
        assert!(
            adapter.observe_native_target_receipt(&next).unwrap_err()
                == super::TranscriptRefusal::Contradicted(NativeLaunchFailure::PartDiffers)
        );
    }

    /// Each part of a Codex message is its own turn; a part is answered when
    /// its turn completes, and the receipt is the model's first output in
    /// the last part's turn.
    #[test]
    fn a_two_part_codex_message_is_received_turn_by_turn() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = two_part_launch(HarnessKind::Codex);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "codex",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        let transcript = root
            .path()
            .join("native-transcripts/codex")
            .join(format!("rollout-{native_session}.jsonl"));
        fs::write(
            &transcript,
            format!("{{\"type\":\"session_meta\",\"payload\":{{\"id\":\"{native_session}\"}}}}\n"),
        )
        .unwrap();
        let intent = registered_intent(&adapter, &launch, pane, native_session);
        let parts = &launch.first_prompt_payload.prompt_part_vector;
        let turn = |turn: &str, text: &str| {
            [
                serde_json::json!({"type":"turn_context","payload":{
                    "turn_id":turn,"model":"model-current","effort":"high"}}),
                serde_json::json!({"type":"event_msg","payload":{"type":"item_completed",
                    "thread_id":native_session,"turn_id":turn,
                    "item":{"type":"UserMessage","content":[{"type":"text","text":text}]}}}),
            ]
        };
        let mut append = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .unwrap();
        for row in turn("turn-1", &parts[0].prompt_part_text) {
            writeln!(append, "{row}").unwrap();
        }
        assert_eq!(
            adapter.observe_native_target_receipt(&intent).unwrap(),
            StartupProgress::Waiting(intent.clone())
        );
        writeln!(
            append,
            "{}",
            serde_json::json!({"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn-1"}})
        )
        .unwrap();
        assert_eq!(
            adapter.observe_native_target_receipt(&intent).unwrap(),
            StartupProgress::PartAnswered(intent.clone())
        );
        let mut next = intent.clone();
        next.parts_submitted = 2;
        for row in turn("turn-2", &parts[1].prompt_part_text) {
            writeln!(append, "{row}").unwrap();
        }
        writeln!(
            append,
            "{}",
            serde_json::json!({"type":"response_item","payload":{"type":"reasoning","summary":[]}})
        )
        .unwrap();
        let StartupProgress::Received(receipt) =
            adapter.observe_native_target_receipt(&next).unwrap()
        else {
            panic!("the last part's turn answering is the receipt");
        };
        assert_eq!(receipt.native_turn_id, "turn-2");
    }

    /// The next part is typed through the socket only once it is counted as
    /// submitted, and it is the part the count names.
    #[test]
    fn the_part_the_intent_counts_is_the_one_typed() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = two_part_launch(HarnessKind::Claude);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        let intent = registered_intent(&adapter, &launch, pane, native_session);
        adapter.submit_startup_part(&launch, &intent).unwrap();
        let mut next = intent.clone();
        next.parts_submitted = 2;
        adapter.submit_startup_part(&launch, &next).unwrap();
        let prompts = socket_prompts(&root);
        assert_eq!(prompts.len(), 2);
        let parts = &launch.first_prompt_payload.prompt_part_vector;
        assert_eq!(
            prompts[0]["params"]["text"],
            parts[0].prompt_part_text.as_str()
        );
        assert_eq!(
            prompts[1]["params"]["text"],
            parts[1].prompt_part_text.as_str()
        );
        assert!(
            prompts[0]["params"]["text"]
                .as_str()
                .unwrap()
                .starts_with("Startup message, part 1 of 2.")
        );
        next.parts_submitted = 3;
        assert!(adapter.submit_startup_part(&launch, &next).is_err());
        assert_eq!(socket_prompts(&root).len(), 2);
    }

    /// A Claude launch reserved its FlowId as Flow does: its session is the
    /// one Flow chose, claimed as 123456 before the harness starts.
    fn reserved_claude(
        launch: &ComposedLaunch,
    ) -> (tempfile::TempDir, HerdrCli, String, HerdrPaneBinding) {
        use crate::herdr::reservation::{ChoosesNativeSession, ReservesFlowIdentity};
        let session = HerdrCli::default()
            .with_ordinary_socket(Path::new("/fixture/run/flow-next/flow/flow.sock"))
            .reserved_native_session_id(launch)
            .expect("a Claude launch chooses its session");
        let agent_name = launch.launch_agent_name();
        let (root, adapter) =
            fixture_herdr("claude", &session, &session.replace('-', ""), &agent_name);
        assert_eq!(
            adapter.reserve_flow_identity(launch).unwrap().as_deref(),
            Some("123456")
        );
        let pane = adapter.create_launch_pane(launch).expect("created pane");
        (root, adapter, session, pane)
    }

    #[test]
    fn claude_is_named_at_launch_and_its_title_read_back() {
        let mut launch = launch(HarnessKind::Claude);
        launch.launch_profile.address.aspect = Aspect::Psyche;
        launch.launch_profile.model_name = "claude-fable-5-1".into();
        launch.leave_outs = vec![signal_flow::LeaveOut::SkillTool];
        let (root, adapter, session, pane) = reserved_claude(&launch);
        adapter
            .start_native_harness(&launch, &pane, Some("123456"))
            .expect("started without prompt");
        let expected = "{ Psyche flow Secondary 123456 }";
        let start = fs::read_to_string(root.path().join("owned-command")).unwrap();
        // The title is a launch argument, beside the slash-command leave-out.
        assert!(
            start.contains(&format!("'--name' {}", expected.shell_quoted())),
            "{start}"
        );
        assert!(start.contains("'--disable-slash-commands'"), "{start}");
        let binding = adapter
            .observe_native_binding(&launch, &pane, Some("123456"))
            .expect("claimed binding");
        let title = adapter
            .title_native_flow(&launch, &binding)
            .expect("title read back");
        assert_eq!(title.as_str(), expected);
        // Readback: the terminal title Herdr reports, the session's
        // transcript title record, and the Herdr pane label all equal it.
        assert_eq!(
            fs::read_to_string(root.path().join("native-title")).unwrap(),
            expected
        );
        assert_eq!(
            adapter.claude_transcript_titles(&pane, &session).unwrap(),
            vec![expected.to_owned()]
        );
        assert_eq!(
            fs::read_to_string(root.path().join("pane-label"))
                .unwrap()
                .trim_end(),
            expected
        );
        // Nothing is typed into the pane to title it.
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        assert!(!calls.contains("agent prompt"), "{calls}");
        assert!(!calls.contains("/rename"), "{calls}");
        let label = calls
            .lines()
            .position(|line| line.ends_with(&format!("pane rename w7:p1 {expected}")))
            .expect("Herdr label");
        let label_read = calls
            .lines()
            .position(|line| line.ends_with("pane get w7:p1"))
            .expect("Herdr label readback");
        assert!(label < label_read);
    }

    #[test]
    fn claude_title_readback_that_differs_refuses_before_the_label() {
        let mut launch = launch(HarnessKind::Claude);
        launch.launch_profile.address.aspect = Aspect::Psyche;
        launch.launch_profile.model_name = "claude-fable-5-1".into();
        let (root, adapter, _, pane) = reserved_claude(&launch);
        // The session shows a title another Flow left behind.
        fs::write(
            root.path().join("native-title-override"),
            "Psyche Opus b87854",
        )
        .unwrap();
        adapter
            .start_native_harness(&launch, &pane, Some("123456"))
            .unwrap();
        let binding = adapter
            .observe_native_binding(&launch, &pane, Some("123456"))
            .unwrap();
        assert_eq!(
            adapter.title_native_flow(&launch, &binding).unwrap_err(),
            TitleRefusal::ReadbackDiffers
        );
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        assert!(!calls.contains("pane rename"));
    }

    /// The terminal title alone is not the title when the transcript's
    /// latest title record names another; a record not yet written does not
    /// refuse.
    #[test]
    fn a_transcript_title_record_that_contradicts_the_name_refuses() {
        let launch = launch(HarnessKind::Claude);
        let (root, adapter, session, pane) = reserved_claude(&launch);
        // Claude writes the record only some reads after it started.
        fs::write(root.path().join("title-record-delay"), "100000").unwrap();
        adapter
            .start_native_harness(&launch, &pane, Some("123456"))
            .unwrap();
        let binding = adapter
            .observe_native_binding(&launch, &pane, Some("123456"))
            .unwrap();
        let title = adapter
            .title_native_flow(&launch, &binding)
            .expect("no record yet contradicts the terminal title");
        assert_eq!(title.as_str(), "{ Field flow Secondary 123456 }");

        let (root, adapter, _, pane) = reserved_claude(&launch);
        fs::write(
            root.path()
                .join("native-transcripts/claude")
                .join(format!("{session}.jsonl")),
            format!(
                "{}\n",
                serde_json::json!({"type":"custom-title","customTitle":"an older name","sessionId":session})
            ),
        )
        .unwrap();
        fs::write(root.path().join("title-record-delay"), "100000").unwrap();
        adapter
            .start_native_harness(&launch, &pane, Some("123456"))
            .unwrap();
        let binding = adapter
            .observe_native_binding(&launch, &pane, Some("123456"))
            .unwrap();
        assert_eq!(
            adapter.title_native_flow(&launch, &binding).unwrap_err(),
            TitleRefusal::ReadbackDiffers
        );
        assert!(!root.path().join("pane-label").exists());
    }

    /// A Claude harness has no name but its title, so a start without a
    /// reserved FlowId is refused before Herdr is called.
    #[test]
    fn an_unreserved_claude_start_is_refused_before_herdr() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).unwrap();
        assert!(matches!(
            adapter.start_native_harness(&launch, &pane, None),
            Err(SpawnFailure::Unattempted(_))
        ));
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        assert!(!calls.contains("pane run"), "{calls}");
        assert!(!calls.contains("agent start"), "{calls}");
    }

    #[test]
    fn a_model_alias_is_titled_from_the_address() {
        let mut launch = launch(HarnessKind::Claude);
        launch.launch_profile.model_name = "haiku".into();
        launch.launch_profile.address.layer = signal_flow::Layer::Quaternary;
        let (root, adapter, _, pane) = reserved_claude(&launch);
        adapter
            .start_native_harness(&launch, &pane, Some("123456"))
            .unwrap();
        let binding = adapter
            .observe_native_binding(&launch, &pane, Some("123456"))
            .unwrap();
        let title = adapter
            .title_native_flow(&launch, &binding)
            .expect("an alias model is titled");
        assert_eq!(title.as_str(), "{ Field flow Quaternary 123456 }");
        assert_eq!(
            fs::read_to_string(root.path().join("pane-label"))
                .unwrap()
                .trim_end(),
            title.as_str()
        );
    }

    #[test]
    fn flow_owned_claude_readiness_uses_its_process_and_reported_pane() {
        let launch = launch(HarnessKind::Claude);
        let (root, adapter, _, pane) = reserved_claude(&launch);
        adapter
            .start_native_harness(&launch, &pane, Some("123456"))
            .unwrap();
        let binding = adapter
            .observe_native_binding(&launch, &pane, Some("123456"))
            .unwrap();
        // A Flow-owned Claude has no Herdr-managed agent session.  Its exact
        // process and the lifecycle-reported idle pane are the readiness
        // witness, without a managed `agent wait`.
        fs::write(root.path().join("missing-agent-session"), "absent").unwrap();
        adapter
            .title_native_flow(&launch, &binding)
            .expect("title read once the target is ready");
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        let process = calls
            .lines()
            .collect::<Vec<_>>()
            .iter()
            .rposition(|line| line.ends_with("pane process-info --pane w7:p1"))
            .expect("exact process observation");
        let label = calls
            .lines()
            .position(|line| line.contains("pane rename w7:p1 "))
            .expect("Herdr label");
        assert!(process < label);
        assert!(!calls.contains("agent wait"));
    }

    #[test]
    fn a_flow_owned_claude_with_another_process_session_is_refused_before_any_rename() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).unwrap();
        adapter
            .start_native_harness(&launch, &pane, Some("123456"))
            .unwrap();
        let binding = adapter
            .observe_native_binding(&launch, &pane, None)
            .unwrap();
        let mut stale = binding.clone();
        stale.native_session_id = "12345678-1234-4abc-8def-000000000000".into();
        assert_eq!(
            adapter.title_native_flow(&launch, &stale).unwrap_err(),
            TitleRefusal::Target(NativeTargetRefusal::IdentityInexact)
        );
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        assert!(!calls.contains("/rename"));
        assert!(!calls.contains("pane rename"));
    }

    const REST_WAIT: &str = "agent wait w7:p1 --until idle --until done --timeout 1000";

    #[test]
    fn a_bind_waits_through_herdr_before_it_reads_the_agent() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).unwrap();
        adapter
            .start_native_harness(&launch, &pane, Some("123456"))
            .unwrap();
        adapter
            .observe_native_binding(&launch, &pane, None)
            .expect("bound once Herdr reports rest");
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        let wait = calls
            .lines()
            .position(|line| line.ends_with(REST_WAIT))
            .expect("bind wait");
        let get = calls
            .lines()
            .position(|line| line.contains("agent get"))
            .expect("agent get");
        assert!(wait < get);
    }

    #[test]
    fn a_bind_whose_pane_never_comes_to_rest_is_refused_not_ready_without_reading_it() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).unwrap();
        adapter
            .start_native_harness(&launch, &pane, Some("123456"))
            .unwrap();
        fs::write(root.path().join("agent-wait-refused"), "timeout").unwrap();
        let error = adapter
            .observe_native_binding(&launch, &pane, None)
            .unwrap_err();
        assert!(error.contains("not ready"), "{error}");
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        assert!(!calls.contains("agent get"));
    }

    /// A reserved Claude launch started, bound and titled, ready for its
    /// first part.
    fn promptable(
        adapter: &HerdrCli,
        launch: &ComposedLaunch,
        native_session: &str,
        pane: HerdrPaneBinding,
    ) -> PromptDeliveryIntent {
        adapter
            .start_native_harness(launch, &pane, Some("123456"))
            .unwrap();
        let binding = adapter
            .observe_native_binding(launch, &pane, Some("123456"))
            .unwrap();
        adapter.title_native_flow(launch, &binding).unwrap();
        let acknowledgement = RegistrationAcknowledgement {
            launch_request_id: binding.launch_request_id.clone(),
            flow_id: binding.flow_id.clone(),
            native_session_id: native_session.into(),
            herdr_pane_binding: binding.herdr_pane_binding.clone(),
        };
        adapter
            .accept_registration(launch, &binding, &acknowledgement)
            .expect("registered intent")
    }

    #[test]
    fn the_first_prompt_waits_through_herdr_after_the_title() {
        let launch = launch(HarnessKind::Claude);
        let (root, adapter, native_session, pane) = reserved_claude(&launch);
        let intent = promptable(&adapter, &launch, &native_session, pane);
        let calls_before = fs::read_to_string(root.path().join("calls")).unwrap();
        let title = calls_before
            .lines()
            .position(|line| line.ends_with("pane get w7:p1"))
            .expect("title readback");
        assert!(socket_prompts(&root).is_empty());
        adapter
            .submit_startup_part(&launch, &intent)
            .expect("typed once at rest");
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        let wait = calls
            .lines()
            .collect::<Vec<_>>()
            .iter()
            .rposition(|line| line.ends_with(REST_WAIT))
            .expect("prompt wait");
        assert!(title < wait, "{calls}");
        assert!(
            wait >= calls_before.lines().count(),
            "the wait follows the title"
        );
        assert_eq!(socket_prompts(&root).len(), 1);
    }

    #[test]
    fn a_first_prompt_whose_composer_never_rests_is_refused_typed_and_nothing_is_typed() {
        let launch = launch(HarnessKind::Claude);
        let (root, adapter, native_session, pane) = reserved_claude(&launch);
        let intent = promptable(&adapter, &launch, &native_session, pane);
        fs::write(root.path().join("agent-wait-refused"), "timeout").unwrap();
        let error = adapter.submit_startup_part(&launch, &intent).unwrap_err();
        assert!(error.contains("not ready"), "{error}");
        assert!(socket_prompts(&root).is_empty());
    }

    #[test]
    fn codex_title_is_set_through_the_app_server_and_read_back() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let mut launch = launch(HarnessKind::Codex);
        launch.launch_profile.model_name = "gpt-6-astra".into();
        let agent_name = launch.launch_agent_name();
        let (root, mut adapter) = fixture_herdr(
            "codex",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let expected = "{ Field flow Secondary 123456 }";
        let frame = |json: &str| {
            let mut bytes = vec![0x81, json.len() as u8];
            bytes.extend_from_slice(json.as_bytes());
            bytes
                .iter()
                .map(|byte| format!("\\{:03o}", byte))
                .collect::<String>()
        };
        let frames = [
            frame(r#"{"id":1,"result":{}}"#),
            frame(r#"{"id":2,"result":{}}"#),
            frame(&format!(
                r#"{{"id":3,"result":{{"thread":{{"id":"{native_session}","name":"{expected}"}}}}}}"#
            )),
        ]
        .join("");
        let proxy = root.path().join("codex-proxy");
        FixtureExecutable {
            path: proxy.clone(),
        }
        .install(&format!(
                "#!/bin/sh\nwhile IFS= read -r line; do line=$(printf '%s' \"$line\" | tr -d '\\r'); [ -z \"$line\" ] && break; done\nprintf '%b' 'HTTP/1.1 101 Switching Protocols\\r\\nUpgrade: websocket\\r\\nConnection: Upgrade\\r\\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\\r\\n\\r\\n'\nprintf '%b' '{frames}'\nsleep 2\n"
            ));
        adapter.codex_endpoints.endpoints[0].client_path = proxy;
        adapter.codex_endpoints.endpoints[0]
            .model_names
            .insert("gpt-6-astra".into());
        adapter.codex_endpoints.endpoints[1]
            .model_names
            .remove("gpt-6-astra");
        let pane = adapter.create_launch_pane(&launch).unwrap();
        let binding = adapter
            .observe_native_binding(&launch, &pane, None)
            .unwrap();
        let title = adapter
            .title_native_flow(&launch, &binding)
            .expect("Codex title set and read back");
        assert_eq!(title.as_str(), expected);
        assert_eq!(
            fs::read_to_string(root.path().join("pane-label"))
                .unwrap()
                .trim_end(),
            expected
        );
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        assert!(!calls.contains("/rename"));
    }

    /// A launch of `kind` claimed as `alias`: started, bound and titled
    /// through the fixture Herdr. Its root, its internal agent name and the
    /// title it was given.
    fn titled_launch(kind: HarnessKind, alias: &str) -> (tempfile::TempDir, String, String) {
        use crate::herdr::reservation::{ChoosesNativeSession, ReservesFlowIdentity};
        use crate::title::TitlesFlow;
        let mut launch = launch(kind.clone());
        let agent_name = launch.launch_agent_name();
        let title = match kind {
            HarnessKind::Claude => {
                let session = HerdrCli::default()
                    .with_ordinary_socket(Path::new("/fixture/run/flow-next/flow/flow.sock"))
                    .reserved_native_session_id(&launch)
                    .expect("a Claude launch chooses its session");
                let (root, adapter) =
                    fixture_herdr("claude", &session, &session.replace('-', ""), &agent_name);
                fs::write(root.path().join("flow-id-alias"), alias).unwrap();
                assert_eq!(
                    adapter.reserve_flow_identity(&launch).unwrap().as_deref(),
                    Some(alias)
                );
                let pane = adapter.create_launch_pane(&launch).expect("created pane");
                adapter
                    .start_native_harness(&launch, &pane, Some(alias))
                    .expect("started");
                let binding = adapter
                    .observe_native_binding(&launch, &pane, Some(alias))
                    .expect("bound");
                let title = adapter
                    .title_native_flow(&launch, &binding)
                    .expect("titled");
                return (root, agent_name, title.as_str().to_owned());
            }
            HarnessKind::Codex => {
                launch.launch_profile.model_name = "gpt-6-astra".into();
                launch.launch_profile.native_title(alias).unwrap()
            }
        };
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let (root, mut adapter) = fixture_herdr(
            "codex",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        fs::write(root.path().join("flow-id-alias"), alias).unwrap();
        let frame = |json: &str| {
            let mut bytes = vec![0x81, json.len() as u8];
            bytes.extend_from_slice(json.as_bytes());
            bytes
                .iter()
                .map(|byte| format!("\\{:03o}", byte))
                .collect::<String>()
        };
        let frames = [
            frame(r#"{"id":1,"result":{}}"#),
            frame(r#"{"id":2,"result":{}}"#),
            frame(&format!(
                r#"{{"id":3,"result":{{"thread":{{"id":"{native_session}","name":"{}"}}}}}}"#,
                title.as_str()
            )),
        ]
        .join("");
        let proxy = root.path().join("codex-proxy");
        FixtureExecutable {
            path: proxy.clone(),
        }
        .install(&format!(
                "#!/bin/sh\nwhile IFS= read -r line; do line=$(printf '%s' \"$line\" | tr -d '\\r'); [ -z \"$line\" ] && break; done\nprintf '%b' 'HTTP/1.1 101 Switching Protocols\\r\\nUpgrade: websocket\\r\\nConnection: Upgrade\\r\\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\\r\\n\\r\\n'\nprintf '%b' '{frames}'\nsleep 2\n"
            ));
        adapter.codex_endpoints.endpoints[0].client_path = proxy;
        adapter.codex_endpoints.endpoints[0]
            .model_names
            .insert("gpt-6-astra".into());
        adapter.codex_endpoints.endpoints[1]
            .model_names
            .remove("gpt-6-astra");
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        let binding = adapter
            .observe_native_binding(&launch, &pane, None)
            .expect("bound");
        let titled = adapter
            .title_native_flow(&launch, &binding)
            .expect("titled");
        assert_eq!(titled, title);
        (root, agent_name, title.as_str().to_owned())
    }

    /// What the living sees of a launch in Herdr: the workspace label, the
    /// pane label and the pane's displayed agent.
    fn visible_labels(root: &tempfile::TempDir) -> [String; 3] {
        ["workspace-label", "pane-label", "pane-display-agent"].map(|file| {
            fs::read_to_string(root.path().join(file))
                .unwrap_or_default()
                .trim_end()
                .to_owned()
        })
    }

    #[test]
    fn both_harnesses_label_the_workspace_with_the_title_and_never_the_agent_name() {
        for kind in [HarnessKind::Claude, HarnessKind::Codex] {
            let (root, agent_name, title) = titled_launch(kind.clone(), "aa034f");
            assert_eq!(title, "{ Field flow Secondary aa034f }", "{kind:?}");
            for label in visible_labels(&root) {
                assert_eq!(label, title, "{kind:?}");
                assert!(!label.contains(&agent_name), "{kind:?}");
            }
            let calls = fs::read_to_string(root.path().join("calls")).unwrap();
            let create = calls
                .lines()
                .find(|line| line.contains("workspace create"))
                .expect("workspace create");
            // The workspace is created unlabelled: no hash label is ever shown.
            assert!(!create.contains("--label"), "{create}");
            assert!(!create.contains(&agent_name), "{create}");
            // The agent name reaches Herdr only as the internal start key;
            // every labelling call carries the title.
            for line in calls
                .lines()
                .filter(|line| line.contains(" rename ") || line.contains("report-metadata"))
            {
                assert!(line.ends_with(&title), "{line}");
                assert!(!line.contains(&agent_name), "{line}");
            }
            let order = |needle: &str| {
                calls
                    .lines()
                    .position(|line| line.contains(needle))
                    .unwrap_or_else(|| panic!("{needle}: {calls}"))
            };
            assert!(order("workspace rename w7 ") < order("workspace get w7"));
            assert!(order("pane report-metadata w7:p1 ") < order("pane get w7:p1"));
        }
    }

    #[test]
    fn a_claude_seat_is_titled_on_workspace_and_pane_agent_before_its_agent_starts() {
        let (root, _, title) = titled_launch(HarnessKind::Claude, "aa034f");
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        let order = |needle: &str| {
            calls
                .lines()
                .position(|line| line.contains(needle))
                .unwrap_or_else(|| panic!("{needle}: {calls}"))
        };
        let start = order("pane run w7:p1 '/fixture/claude'");
        assert!(order("workspace rename w7 ") < start, "{title}: {calls}");
        assert!(order("pane report-metadata w7:p1 ") < start, "{calls}");
    }

    #[test]
    fn two_flows_of_one_seat_get_distinct_workspace_labels() {
        for kind in [HarnessKind::Claude, HarnessKind::Codex] {
            let (first_root, first_agent, first) = titled_launch(kind.clone(), "123456");
            let (second_root, _, second) = titled_launch(kind.clone(), "abcdef");
            assert_eq!(first, "{ Field flow Secondary 123456 }");
            assert_eq!(second, "{ Field flow Secondary abcdef }");
            let first_labels = visible_labels(&first_root);
            let second_labels = visible_labels(&second_root);
            for (one, other) in first_labels.iter().zip(second_labels.iter()) {
                assert_ne!(one, other, "{kind:?}");
            }
            assert_eq!(first_labels[0], first);
            assert_eq!(second_labels[0], second);
            // One seat, one internal agent name: only the title tells them apart.
            assert!(!first_labels.concat().contains(&first_agent));
        }
    }

    #[test]
    fn a_refused_workspace_label_refuses_the_title() {
        let launch = launch(HarnessKind::Claude);
        let (root, adapter, _, pane) = reserved_claude(&launch);
        fs::write(root.path().join("workspace-rename-refused"), "refused").unwrap();
        adapter
            .start_native_harness(&launch, &pane, Some("123456"))
            .unwrap();
        let binding = adapter
            .observe_native_binding(&launch, &pane, Some("123456"))
            .unwrap();
        assert_eq!(
            adapter.title_native_flow(&launch, &binding).unwrap_err(),
            TitleRefusal::WorkspaceLabelRefused
        );
    }

    #[test]
    fn a_displayed_agent_that_is_not_the_title_refuses_the_pane_label() {
        let launch = launch(HarnessKind::Claude);
        let (root, adapter, _, pane) = reserved_claude(&launch);
        fs::write(root.path().join("display-agent-refused"), "refused").unwrap();
        adapter
            .start_native_harness(&launch, &pane, Some("123456"))
            .unwrap();
        let binding = adapter
            .observe_native_binding(&launch, &pane, Some("123456"))
            .unwrap();
        assert_eq!(
            adapter.title_native_flow(&launch, &binding).unwrap_err(),
            TitleRefusal::PaneLabelRefused
        );
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        // Only the early naming at spawn; the refused title step never
        // reached the workspace label.
        assert_eq!(calls.matches("workspace rename").count(), 1, "{calls}");
        assert!(!calls.contains("workspace get"), "{calls}");
    }
}
