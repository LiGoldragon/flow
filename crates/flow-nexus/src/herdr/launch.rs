//! Staged native launch through Herdr's documented CLI surface.
//!
//! Launching, claiming an identity, registering it, submitting each part of
//! the first USER message, and observing its target-side receipt are
//! separate operations. The adapter never chooses registration policy and
//! never retries a part whose write may already have succeeded. A part is
//! typed through Herdr's socket API, one JSON request line, never as a
//! process argument.

use super::{DecodesFlowClaim, FlowClaim, HerdrCli};
use crate::codex::NamesBoundCodexThread;
use crate::codex::SelectsCodexEndpoint;
use crate::composition::{FitsOneMessage, NamesRemoteControl, ValidatesComposedPrompt};
pub use crate::generated::starter::StartupProgress;
use crate::generated::starter::{LeanStart, ReadsTypedUserText, RendersLeanStart, StartupReading};
use crate::herdr::reservation::ChoosesNativeSession;
use crate::title::{NativeTitle, ShowsNativeTitle, TitlesFlow};
use sha2::{Digest, Sha256};
use signal_flow::{
    ComposedLaunch, HarnessKind, HerdrPaneBinding, NativeLaunchBinding, NativeTargetReceipt,
    NativeTranscriptAbsence, NativeTranscriptBoundary, NativeTranscriptCursor,
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
    ) -> Result<(), String>;
}

/// Starts a Claude flow's harness again, in a new pane, as the native
/// session it already had: `--resume <session>` where a launch passes
/// `--session-id`. Claude Code 2.1.294's own help says resuming reuses the
/// original session ID unless `--fork-session` is given, so the resumed
/// harness writes the same transcript and the flow keeps its FlowId, which
/// is exported as FLOW_ID exactly as at launch. Codex is not resumed here.
pub trait ResumesNativeHerdrHarness {
    fn resume_native_harness(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
        flow_id: &str,
        native_session_id: &str,
    ) -> Result<(), String>;
}

/// How a Claude harness is told its native session at spawn.
enum ClaudeSession {
    /// Claude chooses its own session.
    Unchosen,
    /// The session Flow chose for a reserved launch: `--session-id`.
    Chosen(String),
    /// The session a sleeping flow had: `--resume`.
    Resumed(String),
}

/// The one Herdr `agent start` both a launch and a resume run.
trait SpawnsNativeHarness {
    fn spawn_native_harness(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
        flow_id: Option<&str>,
        session: ClaudeSession,
    ) -> Result<(), String>;
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
    #[error("Herdr refused the native rename")]
    RenameRefused,
    #[error("the native transcript title is unreadable")]
    TranscriptUnreadable,
    #[error("Claude native title readback differs from the set title")]
    ReadbackDiffers,
    #[error("the Codex app-server refused the thread name")]
    CodexNamingRefused,
    #[error("Herdr pane label readback differs from the title")]
    PaneLabelRefused,
}

/// After the Flow ID is claimed, sets the canonical native title and the
/// Herdr pane label, and reads both back before any prompt is authorized.
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
    ) -> Result<PromptDeliveryIntent, String>;
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
    ) -> Result<StartupProgress, String>;
}

/// Herdr's socket API, spoken directly: one JSON request line out, one JSON
/// reply line back, on the socket `herdr session list` names for the pane's
/// session. Herdr 0.8.2 reads a request line up to 1 MiB.
pub(crate) trait PromptsThroughHerdrSocket {
    const PROMPT_REPLY_TIMEOUT: Duration = Duration::from_secs(30);
    fn herdr_session_socket(&self, herdr_session_name: &str) -> Result<PathBuf, String>;
    /// Herdr's `agent.prompt` for the pane's agent: the text, then Enter.
    fn prompt_through_socket(&self, pane: &HerdrPaneBinding, text: &str) -> Result<(), String>;
}

impl PromptsThroughHerdrSocket for HerdrCli {
    fn herdr_session_socket(&self, herdr_session_name: &str) -> Result<PathBuf, String> {
        use crate::caller::KnowsHerdrServers;
        self.herdr_sessions()
            .ok_or_else(|| "Herdr sessions could not be listed".to_owned())?
            .into_iter()
            .find(|session| session.name == herdr_session_name)
            .map(|session| session.socket)
            .ok_or_else(|| "Herdr lists no socket for the pane's session".to_owned())
    }

    fn prompt_through_socket(&self, pane: &HerdrPaneBinding, text: &str) -> Result<(), String> {
        let request = serde_json::json!({
            "id": format!("flow:prompt:{}", pane.launch_request_id),
            "method": "agent.prompt",
            "params": { "target": pane.herdr_agent_name, "text": text }
        });
        let mut line = serde_json::to_string(&request)
            .map_err(|error| format!("Herdr prompt request not encoded: {error}"))?;
        line.push('\n');
        if line.len() > HarnessKind::HERDR_REQUEST_LINE_LIMIT {
            return Err("Herdr prompt request exceeds its line limit".into());
        }
        let socket = self.herdr_session_socket(&pane.herdr_session_name)?;
        let mut stream = UnixStream::connect(&socket)
            .map_err(|error| format!("Herdr socket unreachable: {error}"))?;
        stream
            .set_read_timeout(Some(Self::PROMPT_REPLY_TIMEOUT))
            .and_then(|_| stream.set_write_timeout(Some(Self::PROMPT_REPLY_TIMEOUT)))
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
        Ok(())
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
    /// SessionStart, every PostToolUse and Stop, so each harness event
    /// reaches Flow as a Report.
    /// A launch that leaves out the attribution reminder sets the
    /// attribution lines empty here.
    fn claude_flag_settings(&self, lean: &LeanStart) -> String;
    const CLAUDE_REMOTE_CONTROL_FLAG: &'static str = "--remote-control";
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
                "Stop": hook,
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
            "50".into(),
            "--timeout".into(),
            "5000".into(),
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
        let session = agent
            .get("agent_session")
            .ok_or(NativeTargetRefusal::NoNativeSession)?;
        let expected_harness = harness.expected_harness();
        if agent.get("agent").and_then(serde_json::Value::as_str) != Some(expected_harness)
            || session.get("source").and_then(serde_json::Value::as_str)
                != Some(format!("herdr:{expected_harness}").as_str())
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
    ) -> Result<(PathBuf, Metadata, Vec<PathBuf>), String>;
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
    fn receipt_input(&self, intent: &PromptDeliveryIntent) -> Result<Option<(File, bool)>, String>;
}

impl CapturesTranscripts for HerdrCli {
    fn transcript_candidates(
        &self,
        pane: &HerdrPaneBinding,
        native_session_id: &str,
        harness: &HarnessKind,
        model_name: Option<&str>,
    ) -> Result<(PathBuf, Metadata, Vec<PathBuf>), String> {
        self.verify_native_target(pane, native_session_id, harness)?;
        let root = match harness {
            HarnessKind::Codex => {
                &self
                    .codex_endpoints
                    .endpoint_for(model_name.ok_or("Codex transcript selection requires a model")?)
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
            return Err("configured native transcript root is not a directory".into());
        }
        let mut found = Vec::new();
        canonical_root.collect_native_transcripts(native_session_id, harness, 0, &mut found)?;
        let root_after = fs::metadata(&canonical_root)
            .map_err(|error| format!("native transcript root metadata failed: {error}"))?;
        if root_before.dev() != root_after.dev() || root_before.ino() != root_after.ino() {
            return Err("configured native transcript root changed during observation".into());
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

    fn receipt_input(&self, intent: &PromptDeliveryIntent) -> Result<Option<(File, bool)>, String> {
        if !intent.boundary_matches_intent() {
            return Err("native transcript boundary does not match prompt intent".into());
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
                    return Err("configured native transcript root was replaced".into());
                }
                if found.is_empty() {
                    return Ok(None);
                }
                let transcript = root.one_resolved_transcript(found)?;
                File::open(transcript)
                    .map(|file| Some((file, true)))
                    .map_err(|error| format!("native transcript is unreadable: {error}"))
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
                    return Err("native transcript file was replaced".into());
                }
                if metadata.len() < byte_offset {
                    return Err("native transcript file was truncated".into());
                }
                let (prefix_sha256, _) = file.hash_prefix(byte_offset)?;
                if prefix_sha256 != cursor.transcript_prefix_sha256 {
                    return Err("native transcript prefix changed after intent persistence".into());
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
        environment: &[String],
    ) -> String;
}

impl PreparesClaudeEnvironment for str {
    fn claude_environment_preparation(
        &self,
        reserved: Option<&str>,
        socket: &Path,
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
            "unset {} && export FLOW_SOCKET={}{flow_id}{leave_outs} && printf 'FLOW_CLAUDE_ENV_READY_%s\\n' {}",
            HerdrCli::CLAUDE_INHERITED_ENVIRONMENT.join(" "),
            socket.to_string_lossy().shell_quoted(),
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
        format!(
            "{}-{}",
            launch.launch_profile.harness_kind.expected_harness(),
            &hash[..24]
        )
    }

    fn lean_start(&self) -> LeanStart {
        LeanStart {
            harness_kind: self.launch_profile.harness_kind.clone(),
            leave_outs: self.leave_outs.clone(),
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
        if !launch.has_canonical_first_prompt() {
            return Err("composed first prompt is not canonical".into());
        }
        launch.binding_matches_launch(&intent.herdr_pane_binding)?;
        if intent.launch_request_id != launch.launch_profile.launch_request_id
            || intent.prompt_sha256 != launch.first_prompt_payload.prompt_sha256
            || intent.harness_kind != launch.launch_profile.harness_kind
            || intent.model_name != launch.launch_profile.model_name
            || intent.effort != launch.launch_profile.effort
            || intent.prompt_sha256_vector != launch.part_hashes()
            || intent.leave_outs != launch.leave_outs
            || !usize::try_from(intent.parts_submitted)
                .is_ok_and(|parts| (1..=intent.prompt_sha256_vector.len()).contains(&parts))
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
    /// The SHA-256 of the exact text, as a part's hash names it. Claude may
    /// store the exact text inside its native pasted-content wrapper, which
    /// is normalized separately.
    fn text_sha256(&self) -> String;
    /// Returns the exact text inside Claude Code's native paste wrapper.
    /// Attributes are deliberately opaque but cannot contain tag or line
    /// delimiters; the wrapper itself is not part of the persisted digest.
    fn claude_pasted_content(&self) -> Option<&str>;
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

    fn claude_pasted_content(&self) -> Option<&str> {
        let text = self;
        let rest = text.strip_prefix("<pasted_content")?;
        let first = rest.chars().next()?;
        if first != '>' && !first.is_whitespace() {
            return None;
        }
        let (attributes, body) = rest.split_once(">\n")?;
        if attributes.contains(['<', '>', '\r', '\n']) {
            return None;
        }
        body.strip_suffix("\n</pasted_content>")
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
    const TITLE_READBACK_ATTEMPTS: usize = 40;
    #[cfg(not(test))]
    const TITLE_READBACK_INTERVAL: Duration = Duration::from_millis(250);
    #[cfg(test)]
    const TITLE_READBACK_INTERVAL: Duration = Duration::from_millis(1);
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
    /// Renames the Claude session with its own `/rename` command and reads
    /// the title back from the terminal title Claude sets and from a
    /// transcript title record written after the command. A session whose
    /// latest title record is already the title is left as it is.
    fn title_claude_session(
        &self,
        binding: &NativeLaunchBinding,
        title: &NativeTitle,
    ) -> Result<(), TitleRefusal>;
    /// Labels the launch pane with the title and reads the label back.
    fn label_herdr_pane(&self, pane: &HerdrPaneBinding, title: &NativeTitle) -> Result<(), String>;
}

impl TitlesClaudeSession for HerdrCli {
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
        // Herdr may report a just-bound Claude before its session and its
        // readiness: it is waited for, never checked once.
        self.await_pane_rest(pane)
            .map_err(|_| TitleRefusal::NotReady)?;
        let mut last = TitleRefusal::NotReady;
        for _ in 0..Self::TITLE_READBACK_ATTEMPTS {
            match self.verify_native_target(pane, &binding.native_session_id, &HarnessKind::Claude)
            {
                Ok(agent)
                    if agent
                        .get("interactive_ready")
                        .and_then(serde_json::Value::as_bool)
                        == Some(true) =>
                {
                    return Ok(agent);
                }
                Ok(_) => last = TitleRefusal::NotReady,
                Err(NativeTargetRefusal::NoNativeSession) => {
                    last = TitleRefusal::Target(NativeTargetRefusal::NoNativeSession);
                }
                Err(refusal) => return Err(refusal.into()),
            }
            std::thread::sleep(Self::TITLE_READBACK_INTERVAL);
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
        // A resumed session keeps the title its first life recorded: when
        // its latest title record is already this title, nothing is typed.
        let before = self
            .claude_transcript_titles(pane, &binding.native_session_id)
            .map_err(|_| TitleRefusal::TranscriptUnreadable)?;
        if before.last().map(String::as_str) == Some(title.as_str()) {
            return Ok(());
        }
        self.run_json(&[
            "--session".into(),
            pane.herdr_session_name.clone(),
            "agent".into(),
            "prompt".into(),
            pane.herdr_agent_name.clone(),
            format!("/rename {}", title.as_str()),
        ])
        .map_err(|_| TitleRefusal::RenameRefused)?;
        for _ in 0..Self::TITLE_READBACK_ATTEMPTS {
            let agent =
                self.verify_native_target(pane, &binding.native_session_id, &HarnessKind::Claude)?;
            let terminal = agent
                .get("terminal_title_stripped")
                .and_then(serde_json::Value::as_str);
            // Only a title record written after this `/rename` counts: one
            // more than before it was typed, naming this title. Claude takes
            // the command only on Herdr's Enter, 300 ms after the text.
            let recorded = self
                .claude_transcript_titles(pane, &binding.native_session_id)
                .map_err(|_| TitleRefusal::TranscriptUnreadable)?;
            if terminal == Some(title.as_str())
                && recorded.len() > before.len()
                && recorded.last().map(String::as_str) == Some(title.as_str())
            {
                return Ok(());
            }
            std::thread::sleep(Self::TITLE_READBACK_INTERVAL);
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
        {
            return Err("Herdr pane label readback differs from the title".into());
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
            || binding.harness_kind != launch.launch_profile.harness_kind
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
            "--label".into(),
            agent_name.clone(),
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
    ) -> Result<(), String> {
        // A reserved FlowId was claimed for the session Flow chose; the
        // harness runs as that session.
        let session =
            match reserved.and_then(|_| self.reserved_native_session_id(&launch.launch_profile)) {
                Some(session) => ClaudeSession::Chosen(session),
                None => ClaudeSession::Unchosen,
            };
        self.spawn_native_harness(launch, pane, reserved, session)
    }
}

impl ResumesNativeHerdrHarness for HerdrCli {
    fn resume_native_harness(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
        flow_id: &str,
        native_session_id: &str,
    ) -> Result<(), String> {
        if launch.launch_profile.harness_kind != HarnessKind::Claude {
            return Err("only a Claude harness is resumed in a pane".into());
        }
        if native_session_id.is_empty() {
            return Err("a resumed harness needs its native session".into());
        }
        self.spawn_native_harness(
            launch,
            pane,
            Some(flow_id),
            ClaudeSession::Resumed(native_session_id.to_owned()),
        )
    }
}

impl SpawnsNativeHarness for HerdrCli {
    fn spawn_native_harness(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
        reserved: Option<&str>,
        session: ClaudeSession,
    ) -> Result<(), String> {
        launch.binding_matches_launch(pane)?;
        if launch.launch_profile.harness_kind == HarnessKind::Claude {
            self.prepare_claude_pane_environment(launch, pane, reserved)?;
        }
        let harness = launch.launch_profile.harness_kind.expected_harness();
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
            "30000".into(),
        ];
        let codex_endpoint = if launch.launch_profile.harness_kind == HarnessKind::Codex {
            let endpoint = self
                .codex_endpoints
                .endpoint_for(&launch.launch_profile.model_name)
                .map_err(|error| error.to_string())?;
            if !endpoint.client_path.is_absolute() || !Path::new(&endpoint.socket).is_absolute() {
                return Err("configured Codex client and socket must be absolute".into());
            }
            arguments.push("--executable".into());
            arguments.push(
                endpoint
                    .client_path
                    .to_str()
                    .ok_or("configured Codex client path must be valid UTF-8")?
                    .into(),
            );
            Some(endpoint)
        } else {
            None
        };
        arguments.push("--".into());
        if let Some(endpoint) = codex_endpoint {
            arguments.push("--remote".into());
            arguments.push(format!("unix://{}", endpoint.socket));
        }
        if launch.launch_profile.harness_kind == HarnessKind::Claude {
            arguments.push("--settings".into());
            arguments.push(self.claude_flag_settings(&launch.lean_start()));
            // Every Claude Flow is remotely controllable; the name never
            // begins with `-`, so the optional value binds to the flag.
            arguments.push(Self::CLAUDE_REMOTE_CONTROL_FLAG.into());
            arguments.push(launch.launch_profile.remote_control_name());
            // The composed system prompt travels as this one argument;
            // nothing composed is written to disk.
            arguments.push("--system-prompt".into());
            arguments.push(
                HarnessKind::Claude.system_argument(&launch.system_prompt.system_prompt_text),
            );
            match session {
                ClaudeSession::Unchosen => {}
                ClaudeSession::Chosen(session) => {
                    arguments.push("--session-id".into());
                    arguments.push(session);
                }
                ClaudeSession::Resumed(session) => {
                    arguments.push("--resume".into());
                    arguments.push(session);
                }
            }
        }
        arguments.push("--model".into());
        arguments.push(launch.launch_profile.model_name.clone());
        match launch.launch_profile.harness_kind {
            HarnessKind::Claude => {
                arguments.push("--effort".into());
                arguments.push(launch.launch_profile.effort.clone());
            }
            HarnessKind::Codex => {
                arguments.push("-c".into());
                arguments.push(format!(
                    "model_reasoning_effort={}",
                    launch.launch_profile.effort
                ));
                // Codex reads `instructions` as its base instructions when
                // no instructions file is configured; the composed system
                // prompt travels in this one argument.
                arguments.push("-c".into());
                arguments.push(
                    HarnessKind::Codex.system_argument(&launch.system_prompt.system_prompt_text),
                );
            }
        }
        // The launch's leave-outs, as its harness's own switches.
        arguments.extend(launch.lean_start().arguments());
        if !launch
            .launch_profile
            .harness_kind
            .fits_one_argument(&launch.system_prompt.system_prompt_text)
        {
            return Err("the composed system prompt exceeds one argument".into());
        }
        // No positional prompt: the first USER message is typed after.
        self.run_json(&arguments).map(|_| ())
    }
}

/// Herdr's own wait for a launched agent's pane to come to rest. The launch
/// path never polls for readiness itself: a bind, and a first prompt after
/// the title, each wait here, bounded, and a timeout is a typed not-ready
/// refusal.
pub(crate) trait WaitsForPaneRest {
    /// How long Herdr's `agent wait` may take before a bind or a first prompt.
    #[cfg(not(test))]
    const REST_WAIT_MILLISECONDS: &'static str = "30000";
    #[cfg(test)]
    const REST_WAIT_MILLISECONDS: &'static str = "1000";
    /// The refusal text a bind or a prompt that never found the pane at rest
    /// carries; the failure classifier reads it as not-ready.
    const NOT_READY: &'static str = "Herdr agent did not come to rest: not ready";
    fn await_pane_rest(&self, pane: &HerdrPaneBinding) -> Result<(), String>;
}

impl WaitsForPaneRest for HerdrCli {
    fn await_pane_rest(&self, pane: &HerdrPaneBinding) -> Result<(), String> {
        self.run_status(&[
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
            Self::REST_WAIT_MILLISECONDS.into(),
        ])
        .map_err(|_| Self::NOT_READY.to_owned())
    }
}

impl ObservesNativeLaunchBinding for HerdrCli {
    fn observe_native_binding(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
        reserved: Option<&str>,
    ) -> Result<NativeLaunchBinding, String> {
        launch.binding_matches_launch(pane)?;
        let expected_harness = launch.launch_profile.harness_kind.expected_harness();
        // Herdr may report a just-started harness before it is ready: it is
        // waited for, never checked once.
        self.await_pane_rest(pane)?;
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
        let exact_pane = agent.get("name").and_then(serde_json::Value::as_str)
            == Some(pane.herdr_agent_name.as_str())
            && agent.get("agent").and_then(serde_json::Value::as_str) == Some(expected_harness)
            && agent
                .get("workspace_id")
                .and_then(serde_json::Value::as_str)
                == Some(pane.herdr_workspace_id.as_str())
            && agent.get("pane_id").and_then(serde_json::Value::as_str)
                == Some(pane.herdr_pane_id.as_str())
            && agent.get("terminal_id").and_then(serde_json::Value::as_str)
                == Some(pane.herdr_terminal_id.as_str())
            && agent
                .get("interactive_ready")
                .and_then(serde_json::Value::as_bool)
                == Some(true);
        if !exact_pane {
            return Err("Herdr agent does not match the created native pane".into());
        }
        // Claude receives the Flow-selected session through `--session-id`.
        // Herdr may not expose `agent_session` before Claude finishes its own
        // initialization, so a reserved Claude launch binds that selected value
        // after exact pane/harness/readiness verification. If Herdr does expose
        // a session, it must agree exactly; it is never silently substituted.
        let selected_claude_session =
            if launch.launch_profile.harness_kind == HarnessKind::Claude && reserved.is_some() {
                self.reserved_native_session_id(&launch.launch_profile)
                    .filter(|session| !session.is_empty())
            } else {
                None
            };
        let expected_source = format!("herdr:{expected_harness}");
        let observed_session = agent.get("agent_session");
        let observed_native_session = |session: &serde_json::Value| -> Result<String, String> {
            if session.get("source").and_then(serde_json::Value::as_str)
                != Some(expected_source.as_str())
                || session.get("agent").and_then(serde_json::Value::as_str)
                    != Some(expected_harness)
                || session.get("kind").and_then(serde_json::Value::as_str) != Some("id")
            {
                return Err(
                    "native identity did not come from the official Herdr integration".into(),
                );
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
        let native_session_id = if let Some(selected) = selected_claude_session.as_deref() {
            if let Some(session) = observed_session {
                let observed = observed_native_session(session)?;
                if observed != selected {
                    return Err(
                        "observed native identity conflicts with the Flow-selected Claude session"
                            .into(),
                    );
                }
            }
            selected.to_owned()
        } else {
            let session = observed_session.ok_or_else(|| {
                "official Herdr integration reported no native session".to_owned()
            })?;
            observed_native_session(session)?
        };
        let normalized_identity = native_session_id
            .bytes()
            .filter(|byte| *byte != b'-')
            .map(char::from)
            .collect::<String>()
            .to_ascii_lowercase();
        if reserved.is_some()
            && self
                .reserved_native_session_id(&launch.launch_profile)
                .as_deref()
                != Some(native_session_id.as_str())
        {
            return Err("native session is not the one Flow reserved for this launch".into());
        }
        let flow_id =
            self.claim_flow_identity(&launch.launch_profile.harness_kind, &native_session_id)?;
        if reserved.is_some_and(|reserved| reserved != flow_id) {
            return Err("flow claim does not name the FlowId Flow reserved".into());
        }
        if self.claimed_native_identity(&flow_id, &launch.launch_profile.harness_kind)?
            != normalized_identity
        {
            return Err("flow claim does not contain the observed native identity".into());
        }
        Ok(NativeLaunchBinding {
            launch_request_id: launch.launch_profile.launch_request_id.clone(),
            flow_id,
            native_session_id,
            harness_kind: launch.launch_profile.harness_kind.clone(),
            herdr_pane_binding: pane.clone(),
        })
    }
}

impl AcceptsLaunchRegistration for HerdrCli {
    fn accept_registration(
        &self,
        launch: &ComposedLaunch,
        binding: &NativeLaunchBinding,
        acknowledgement: &RegistrationAcknowledgement,
    ) -> Result<PromptDeliveryIntent, String> {
        launch.binding_matches_launch(&binding.herdr_pane_binding)?;
        if binding.harness_kind != launch.launch_profile.harness_kind
            || acknowledgement.launch_request_id != binding.launch_request_id
            || acknowledgement.flow_id != binding.flow_id
            || acknowledgement.native_session_id != binding.native_session_id
            || acknowledgement.herdr_pane_binding != binding.herdr_pane_binding
        {
            return Err("registration acknowledgement does not match native binding".into());
        }
        if !launch.has_canonical_first_prompt() {
            return Err("composed first prompt is not canonical".into());
        }
        let native_transcript_boundary =
            self.capture_transcript_boundary(binding, &launch.launch_profile.model_name)?;
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
            // The first part is the one about to be submitted. The caller
            // sets the Stop count its flow has reported so far.
            parts_submitted: 1,
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
        // The title's `/rename` (for the first part) or the answer to the
        // part before has come to rest; nothing is typed when it has not.
        self.await_pane_rest(&durable_intent.herdr_pane_binding)?;
        self.prompt_through_socket(&durable_intent.herdr_pane_binding, &part.prompt_part_text)?;
        Ok(PromptDeliveryResult::Ambiguous(durable_intent.clone()))
    }
}

/// One transcript record read against the intent.
trait ReadsStartupRecord {
    /// The receipt turn when this record is the flow's first answer after
    /// every part; a contradiction refuses.
    fn read_record(
        &mut self,
        row: &serde_json::Value,
        intent: &PromptDeliveryIntent,
    ) -> Result<Option<String>, String>;
    /// The part this user text is, when it is the next one expected; a
    /// user text that is not refuses while parts are still expected.
    fn read_part(&mut self, text: &str, intent: &PromptDeliveryIntent) -> Result<(), String>;
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

impl ReadsStartupRecord for StartupReading {
    fn read_part(&mut self, text: &str, intent: &PromptDeliveryIntent) -> Result<(), String> {
        let expected = usize::try_from(self.seen_parts)
            .ok()
            .and_then(|seen| intent.prompt_sha256_vector.get(seen))
            .filter(|_| self.seen_parts < intent.parts_submitted);
        if expected.is_some_and(|hash| &text.text_sha256() == hash) {
            self.seen_parts += 1;
            self.last_part_answered = false;
            return Ok(());
        }
        Err("a native user turn differs from the startup parts".into())
    }

    fn read_record(
        &mut self,
        row: &serde_json::Value,
        intent: &PromptDeliveryIntent,
    ) -> Result<Option<String>, String> {
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
                    return Err("the native session shows a part its leave-outs removed".into());
                }
                if kind == Some("user")
                    && !every_part
                    && let Some(text) = row.claude_typed_text()
                {
                    let original = text.claude_pasted_content().unwrap_or(&text).to_owned();
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
                            return Err("native Claude model or effort differs from intent".into());
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
                if kind == Some("turn_context") {
                    let payload = payload
                        .ok_or_else(|| "native Codex turn context has no payload".to_owned())?;
                    if payload.get("model").and_then(serde_json::Value::as_str)
                        != Some(intent.model_name.as_str())
                        || payload.get("effort").and_then(serde_json::Value::as_str)
                            != Some(intent.effort.as_str())
                    {
                        return Err("native Codex model or effort differs from intent".into());
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
                        .ok_or_else(|| "native Codex user turn has no input vector".to_owned())?;
                    let text = match content.as_slice() {
                        [item]
                            if item.get("type").and_then(serde_json::Value::as_str)
                                == Some("text") =>
                        {
                            item.get("text").and_then(serde_json::Value::as_str)
                        }
                        _ => None,
                    }
                    .ok_or_else(|| "native Codex user turn is not one text item".to_owned())?;
                    if every_part || turn.is_none() || turn != self.native_turn_option.as_deref() {
                        return Err("native Codex user turn differs from the startup parts".into());
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
    ) -> Result<StartupProgress, String> {
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
                return Err("new native transcript disappeared during adoption".into());
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
    use crate::composition::{FitsOneMessage, NamesRemoteControl, SplitsStartupMessage};
    use crate::fixture_executable::{FixtureExecutable, InstallsScript};
    use crate::herdr::ConfiguresHerdrCli;
    use crate::herdr::HerdrCli;
    use crate::herdr::launch::ChecksComposedLaunch;
    use crate::herdr::launch::PreparesClaudeEnvironment;
    use crate::herdr::launch::PreparesClaudePane;
    use crate::herdr::launch::TitlesClaudeSession;
    use crate::herdr::launch::{NativeTargetRefusal, TitleRefusal};
    use crate::title::ShowsNativeTitle;
    use signal_flow::{
        Aspect, ComposedLaunch, Effort, FirstPromptPayload, HarnessKind, LaunchProfile, ModelName,
        NativeLaunchBinding, NativeTargetReceipt, NativeTranscriptBoundary, PowerLevel,
        PromptDeliveryIntent, PromptDeliveryResult, PromptPart, RegistrationAcknowledgement,
        SystemPrompt, TargetReceiptRequest,
    };
    use std::fs;
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixListener;
    use std::path::Path;

    const SYSTEM_TEXT: &str = "fixture system prompt";

    fn digest(text: &str) -> String {
        format!(
            "{:x}",
            <sha2::Sha256 as sha2::Digest>::digest(text.as_bytes())
        )
    }

    const PROMPT_HASH: &str = "0cb26cfe0a554e4780aa5af20cafbe3ae3259f823438576026a4ffff58371a67";

    pub(crate) fn launch(harness_kind: HarnessKind) -> ComposedLaunch {
        ComposedLaunch {
            launch_profile: LaunchProfile {
                launch_request_id: "launch-42".into(),
                system_modules: vec!["spirit-core".into(), "intent-flow".into()],
                user_modules: vec!["operation-main-flow".into()],
                address: signal_flow::Address {
                    aspect: Aspect::Field,
                    topic: "flow".into(),
                    layer: signal_flow::Layer::Secondary,
                },
                power_level: PowerLevel::Medium,
                harness_kind,
                model_name: ModelName::from("model-current"),
                effort: Effort::from("high"),
                flow_id_option: None,
                remembered_flow_vector: vec![],
                herdr_session_name: "flowlaunch42".into(),
                leave_outs_option: None,
                instruction_prompt: "do the work".into(),
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
                let text = format!("{}{chunk}", HarnessKind::part_header(index + 1, 2));
                PromptPart {
                    prompt_sha256: digest(&text),
                    prompt_part_text: text,
                }
            })
            .collect();
        launch
    }

    fn fixture_herdr(
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
  *"pane run"*) sh -c "$6" > '{marker_output}'; width=$(cat '{pane_width}' 2>/dev/null || printf 120); fold -w "$width" '{marker_output}' > '{visible_output}' ;;
  *"pane wait-output"*) case "$*" in *"--source recent-unwrapped"*) ;; *) exit 9 ;; esac; marker=$(printf '%s\n' "$*" | sed 's/.*--match \([^ ]*\).*/\1/'); line=$(grep -Fx "$marker" '{marker_output}') || exit 10; printf '{{"result":{{"pane_id":"w7:p1","matched_line":"%s"}}}}\n' "$line" ;;
  *"agent wait"*) [ -f '{wait_refused}' ] && exit 11; rm -f '{missing_agent_session}'; printf '%s\n' '{{"result":{{"status":"idle"}}}}' ;;
  *"agent start"*) printf '%s\n' '{{"result":{{"agent":{{"name":"{agent_name}"}}}}}}' ;;
  *"agent get"*) if [ -f '{pending_record}' ]; then left=$(cat '{record_delay}'); if [ "$left" -le 0 ]; then cat '{pending_record}' >> '{claude_transcript}'; rm -f '{pending_record}'; else printf '%s' $((left - 1)) > '{record_delay}'; fi; fi; reported_harness=$(cat '{reported_harness}'); title=$(cat '{title_file}' 2>/dev/null); if [ -f '{missing_agent_session}' ]; then printf '{{"result":{{"agent":{{"name":"{agent_name}","agent":"%s","workspace_id":"w7","pane_id":"w7:p1","terminal_id":"term-native","interactive_ready":true,"terminal_title_stripped":"%s"}}}}}}\n' "$reported_harness" "$title"; else printf '{{"result":{{"agent":{{"name":"{agent_name}","agent":"%s","workspace_id":"w7","pane_id":"w7:p1","terminal_id":"term-native","interactive_ready":true,"terminal_title_stripped":"%s","agent_session":{{"source":"herdr:%s","agent":"%s","kind":"id","value":"{native_session}"}}}}}}}}\n' "$reported_harness" "$title" "$reported_harness" "$reported_harness"; fi ;;
  *"agent prompt"*" /rename "*) title=$(printf '%s' "$*" | sed 's/.* \/rename //'); [ -f '{title_override}' ] && title=$(cat '{title_override}'); printf '%s' "$title" > '{title_file}'; record=$(printf '{{"type":"custom-title","customTitle":"%s","sessionId":"{native_session}"}}' "$title"); if [ -f '{record_delay}' ]; then printf '%s\n' "$record" > '{pending_record}'; else printf '%s\n' "$record" >> '{claude_transcript}'; fi; printf '%s\n' '{{"result":{{"accepted":true}}}}' ;;
  *"agent prompt"*) printf '%s\n' '{{"result":{{"accepted":true}}}}' ;;
  *"pane rename w7:p1 "*) printf '%s' "$*" | sed 's/.*pane rename w7:p1 //' > '{label_file}'; printf '%s\n' '{{"result":{{}}}}' ;;
  *"pane get w7:p1"*) label=$(cat '{label_file}' 2>/dev/null); printf '{{"result":{{"pane":{{"pane_id":"w7:p1","terminal_id":"term-native","label":"%s"}}}}}}\n' "$label" ;;
  "session list --json") printf '%s\n' '{{"sessions":[{{"name":"flowlaunch42","socket_path":"{herdr_socket}"}}]}}' ;;
  *) exit 8 ;;
esac
"##,
            calls = calls.display(),
            reported_harness = reported_harness.display(),
            missing_agent_session = root.path().join("missing-agent-session").display(),
            wait_refused = root.path().join("agent-wait-refused").display(),
            marker_output = root.path().join("marker-unwrapped").display(),
            visible_output = root.path().join("marker-visible").display(),
            pane_width = root.path().join("pane-width").display(),
            title_file = root.path().join("native-title").display(),
            title_override = root.path().join("native-title-override").display(),
            record_delay = root.path().join("title-record-delay").display(),
            pending_record = root.path().join("title-record-pending").display(),
            label_file = root.path().join("pane-label").display(),
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
        let claim_marker = format!(
            "version=1\nharness={harness}\nidentity={claimed_identity}\nalias=123456\n{uuid_version}"
        );
        let flow_id_script = format!(
            r##"#!/bin/sh
printf '%s|%s\n' "$*" "$CODEX_SESSION_ID" >> '{flow_id_calls}'
printf '%s' '{claim_marker}' > '{flows}/.123456.flow-id'
printf '%s\n' 123456
"##,
            flow_id_calls = flow_id_calls.display(),
            flows = flows.display(),
        );
        FixtureExecutable {
            path: flow_id.clone(),
        }
        .install(&flow_id_script);
        let adapter = HerdrCli::at(executable, flows);
        (root, adapter)
    }

    fn registered_intent(
        adapter: &HerdrCli,
        launch: &ComposedLaunch,
        pane: signal_flow::HerdrPaneBinding,
        native_session: &str,
    ) -> PromptDeliveryIntent {
        let binding = NativeLaunchBinding {
            launch_request_id: launch.launch_profile.launch_request_id.clone(),
            flow_id: "123456".into(),
            native_session_id: native_session.into(),
            harness_kind: launch.launch_profile.harness_kind.clone(),
            herdr_pane_binding: pane.clone(),
        };
        let acknowledgement = RegistrationAcknowledgement {
            launch_request_id: binding.launch_request_id.clone(),
            flow_id: binding.flow_id.clone(),
            native_session_id: binding.native_session_id.clone(),
            herdr_pane_binding: pane,
        };
        adapter
            .accept_registration(launch, &binding, &acknowledgement)
            .expect("registered prompt intent")
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
        let instructions = HarnessKind::Codex.system_argument(SYSTEM_TEXT);
        assert_eq!(instructions, format!("instructions=\"{SYSTEM_TEXT}\""));
        assert!(start.contains(&format!("-c {instructions}")));
        assert!(!start.contains("model_instructions_file"));
        assert!(!start.contains("--remote-control"));
        assert_eq!(start.matches("--remote").count(), 1);
        assert!(!calls.contains("pane run"));
        assert!(!start.contains(HerdrCli::CLAUDE_SKIP_PERMISSIONS_FLAG));
        // The harness starts with no positional prompt; the one first prompt
        // travels later in a single app-server turn/start.
        assert!(start.ends_with(&format!("-c {instructions}")));
        assert!(!start.contains('$'));
        assert!(!calls.contains("agent prompt"));
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
            .start_native_harness(&launch, &pane, None)
            .expect("started");
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        assert!(calls.contains(" --disable-slash-commands"));
        assert!(calls.contains(" --disallowedTools Skill,Workflow"));
        assert!(calls.contains(r#""attribution":{"commit":"","pr":""}"#));
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
            adapter
                .observe_native_target_receipt(&intent)
                .unwrap_err()
                .contains("leave-outs removed")
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
            ("test-marker").claude_environment_preparation(None, Path::new("/run/flow.sock"), &[]),
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
            .reserved_native_session_id(&launch.launch_profile)
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
        assert!(prepared.contains(
            " FLOW_ID && export FLOW_SOCKET='/fixture/run/flow-next/flow/flow.sock' && export FLOW_ID=123456 && printf "
        ));
        let start = calls
            .lines()
            .find(|line| line.contains("agent start"))
            .expect("agent start");
        assert!(start.contains(&format!(" --session-id {session} ")));
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
        assert!(
            adapter
                .observe_native_binding(&launch, &pane, Some("abcdef"))
                .unwrap_err()
                .contains("does not name the FlowId Flow reserved")
        );
    }

    #[test]
    fn reserved_claude_binds_selected_session_without_herdr_session_and_refuses_mismatches() {
        use crate::herdr::reservation::{ChoosesNativeSession, ReservesFlowIdentity};
        let launch = launch(HarnessKind::Claude);
        let session = HerdrCli::default()
            .with_ordinary_socket(Path::new("/fixture/run/flow-next/flow/flow.sock"))
            .reserved_native_session_id(&launch.launch_profile)
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
        let binding = adapter
            .observe_native_binding(&launch, &pane, Some("123456"))
            .expect("selected session binds without Herdr session");
        assert_eq!(binding.native_session_id, session);
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        assert!(calls.contains(&format!(" --session-id {session} ")));

        let mut wrong_pane = pane.clone();
        wrong_pane.herdr_pane_id = "w7:p9".into();
        assert!(
            adapter
                .observe_native_binding(&launch, &wrong_pane, Some("123456"))
                .unwrap_err()
                .contains("does not match the created native pane")
        );

        fs::write(root.path().join("reported-harness"), "codex").unwrap();
        assert!(
            adapter
                .observe_native_binding(&launch, &pane, Some("123456"))
                .unwrap_err()
                .contains("does not match the created native pane")
        );
    }

    #[test]
    fn a_reserved_claude_launch_that_came_up_as_another_session_is_not_bound() {
        let launch = launch(HarnessKind::Claude);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            "12345678-1234-4abc-8def-123456789abc",
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        adapter
            .start_native_harness(&launch, &pane, None)
            .expect("started");
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        // Unreserved: no FlowId exported, no session chosen; the launching
        // Nexus's socket is exported all the same.
        assert!(!calls.contains("export FLOW_ID"));
        assert!(
            calls.contains(
                " && export FLOW_SOCKET='/fixture/run/flow-next/flow/flow.sock' && printf "
            )
        );
        assert!(!calls.contains("--session-id"));
        assert!(
            adapter
                .observe_native_binding(&launch, &pane, Some("123456"))
                .unwrap_err()
                .contains("conflicts with the Flow-selected Claude session")
        );
    }

    #[test]
    fn stages_pane_native_claim_registration_and_one_ambiguous_prompt_write() {
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
            .start_native_harness(&launch, &pane, None)
            .expect("started without prompt");
        let calls_after_start =
            fs::read_to_string(root.path().join("calls")).expect("launch calls");
        let start_call = calls_after_start
            .lines()
            .find(|line| line.contains("agent start"))
            .expect("agent start call");
        let remote_control_name = launch.launch_profile.remote_control_name();
        assert!(remote_control_name.starts_with("flow-"));
        assert!(start_call.ends_with(&format!(
            "-- --settings {} --remote-control {remote_control_name} --system-prompt {SYSTEM_TEXT} --model model-current --effort high",
            concat!(
                r#"{"hooks":{"#,
                r#""PostToolUse":[{"hooks":[{"command":"/fixture/bin/flow-hook","type":"command"}],"matcher":"*"}],"#,
                r#""SessionStart":[{"hooks":[{"command":"/fixture/bin/flow-hook","type":"command"}]}],"#,
                r#""Stop":[{"hooks":[{"command":"/fixture/bin/flow-hook","type":"command"}]}]},"#,
                r#""permissions":{"defaultMode":"bypassPermissions"}}"#
            ),
        )));
        // The flag settings name the launch's own mode, which suppresses the
        // auto-mode default offer, and carry the harness hook on SessionStart,
        // every PostToolUse and Stop; no settings file is written.
        assert_eq!(start_call.matches(" --settings ").count(), 1);
        // Each flag once in the pane's argv: the installed wrapper execs
        // `.claude-wrapped --dangerously-skip-permissions "$@"`, so Flow's
        // own arguments never carry that flag.
        let flow_arguments = start_call
            .split_once(" -- ")
            .expect("agent arguments")
            .1
            .split(' ')
            .filter(|argument| !SYSTEM_TEXT.split(' ').any(|word| word == *argument))
            .collect::<Vec<_>>();
        let pane_argv = std::iter::once(HerdrCli::CLAUDE_SKIP_PERMISSIONS_FLAG)
            .chain(flow_arguments.iter().copied())
            .collect::<Vec<_>>();
        for flag in pane_argv
            .iter()
            .filter(|argument| argument.starts_with("--"))
        {
            assert_eq!(
                pane_argv
                    .iter()
                    .filter(|argument| *argument == flag)
                    .count(),
                1,
                "{flag} appears more than once in {pane_argv:?}"
            );
        }
        assert!(!start_call.contains("launch-42"));
        assert!(!start_call.contains("composed body"));
        let pane_run = calls_after_start
            .lines()
            .position(|line| line.contains("pane run"))
            .expect("Claude environment preparation");
        let pane_wait = calls_after_start
            .lines()
            .position(|line| line.contains("pane wait-output"))
            .expect("Claude environment preparation receipt");
        let agent_start = calls_after_start
            .lines()
            .position(|line| line.contains("agent start"))
            .expect("Claude start");
        assert!(pane_run < pane_wait && pane_wait < agent_start);
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
        assert!(matches!(
            &intent.native_transcript_boundary,
            NativeTranscriptBoundary::Absent(_)
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
        assert_eq!(prompts[0]["params"]["target"], agent_name.as_str());
        assert_eq!(prompts[0]["params"]["text"], "composed body");
        let all_calls = fs::read_to_string(root.path().join("calls")).expect("all launch calls");
        assert!(!all_calls.contains("agent prompt"));
    }

    #[test]
    fn malformed_full_prompt_is_rejected_before_herdr_prompt_write() {
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
        let unchanged_body_hash = launch.first_prompt_payload.prompt_sha256.clone();
        let mut malformed = launch;
        malformed.first_prompt_payload.prompt_part_vector[0]
            .prompt_part_text
            .push('x');

        assert_eq!(
            malformed.first_prompt_payload.prompt_sha256,
            unchanged_body_hash
        );
        assert!(
            adapter
                .submit_startup_part(&malformed, &intent)
                .unwrap_err()
                .contains("not canonical")
        );
        assert!(socket_prompts(&root).is_empty());
        let _ = root;
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

    #[test]
    fn a_first_turn_before_the_cursor_is_ignored_and_the_new_one_is_observed() {
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
        let stale_context = serde_json::json!({"type":"turn_context","payload":{
            "turn_id":"turn-stale","model":"model-current","effort":"high"}});
        let stale_input = serde_json::json!({"type":"event_msg","payload":{
            "thread_id":native_session,"turn_id":"turn-stale",
            "item":{"type":"UserMessage","content":[{"type":"text","text":launch.first_prompt_payload.first_prompt_body}]}}});
        fs::write(&transcript, format!("{stale_context}\n{stale_input}\n"))
            .expect("stale transcript");
        let intent = registered_intent(&adapter, &launch, pane, native_session);
        assert!(matches!(
            &intent.native_transcript_boundary,
            NativeTranscriptBoundary::Existing(_)
        ));
        assert_eq!(
            adapter
                .observe_native_target_receipt(&intent)
                .expect("a first turn before the cursor is ignored"),
            StartupProgress::Waiting(intent.clone())
        );
        let context = serde_json::json!({"type":"turn_context","payload":{
            "turn_id":"turn-new","model":"model-current","effort":"high"}});
        let input = serde_json::json!({"type":"event_msg","payload":{
            "thread_id":native_session,"turn_id":"turn-new",
            "item":{"type":"UserMessage","content":[{"type":"text","text":launch.first_prompt_payload.first_prompt_body}]}}});
        use std::io::Write;
        let mut append = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .expect("append transcript");
        writeln!(append, "{context}").expect("append turn context");
        writeln!(append, "{input}").expect("append native input");
        // The part alone is not the receipt: the flow has not answered.
        assert_eq!(
            adapter.observe_native_target_receipt(&intent).unwrap(),
            StartupProgress::Waiting(intent.clone())
        );
        let answer = serde_json::json!({"type":"response_item","payload":{
            "type":"message","role":"assistant","content":[{"type":"output_text","text":"Beginning."}]}});
        writeln!(append, "{answer}").expect("append the first answer");
        let result = adapter
            .observe_native_target_receipt(&intent)
            .expect("observed transcript receipt");
        let StartupProgress::Received(NativeTargetReceipt {
            native_turn_id,
            receipt_sha256,
            ..
        }) = result
        else {
            panic!("receipt remained ambiguous");
        };
        assert_eq!(native_turn_id, "turn-new");
        assert_eq!(receipt_sha256.len(), 64);
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
        fs::write(&transcript, "{\"type\":\"session_meta\"}\n").expect("initial transcript");
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
            adapter
                .observe_native_target_receipt(&intent)
                .unwrap_err()
                .contains("differs from the startup parts")
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
            adapter
                .observe_native_target_receipt(&intent)
                .unwrap_err()
                .contains("model or effort differs")
        );
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
                .contains("native identity is no longer exact")
        );
    }

    #[test]
    fn persisted_cursor_rejects_truncation_prefix_change_and_replacement() {
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
        fs::write(&transcript, "{\"type\":\"user\"}\n").expect("initial transcript");
        let intent = registered_intent(&adapter, &launch, pane, native_session);

        fs::write(&transcript, "").expect("truncate transcript");
        assert!(
            adapter
                .observe_native_target_receipt(&intent)
                .unwrap_err()
                .contains("truncated")
        );

        fs::write(&transcript, "{\"type\":\"xxxx\"}\n").expect("rewrite transcript prefix");
        assert!(
            adapter
                .observe_native_target_receipt(&intent)
                .unwrap_err()
                .contains("prefix changed")
        );

        let replacement = root.path().join("replacement.jsonl");
        fs::write(&replacement, "{\"type\":\"user\"}\n").expect("replacement transcript");
        fs::rename(&replacement, &transcript).expect("replace transcript inode");
        assert!(
            adapter
                .observe_native_target_receipt(&intent)
                .unwrap_err()
                .contains("file was replaced")
        );
    }

    /// Claude Code keeps a long typed message inside its pasted-content
    /// wrapper; the wrapper is taken off before the part's hash is read.
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
                "<pasted_content id=\"1\">\n{}\n</pasted_content>",
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
            adapter
                .observe_native_target_receipt(&next)
                .unwrap_err()
                .contains("differs from the startup parts")
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
        fs::write(&transcript, "{\"type\":\"session_meta\"}\n").unwrap();
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

    #[test]
    fn claude_title_is_set_after_the_claim_and_read_back() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let mut launch = launch(HarnessKind::Claude);
        launch.launch_profile.address.aspect = Aspect::Psyche;
        launch.launch_profile.model_name = "claude-fable-5-1".into();
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        adapter
            .start_native_harness(&launch, &pane, None)
            .expect("started without prompt");
        let binding = adapter
            .observe_native_binding(&launch, &pane, None)
            .expect("claimed binding");
        let title = adapter
            .title_native_flow(&launch, &binding)
            .expect("title set and read back");
        let expected = "{ Psyche flow Secondary 123456 }";
        assert_eq!(title.as_str(), expected);
        // Readback: the terminal title Herdr reports, the session's
        // transcript title record, and the Herdr pane label all equal it.
        assert_eq!(
            fs::read_to_string(root.path().join("native-title")).unwrap(),
            expected
        );
        assert_eq!(
            adapter
                .claude_transcript_titles(&pane, native_session)
                .unwrap(),
            vec![expected.to_owned()]
        );
        assert_eq!(
            fs::read_to_string(root.path().join("pane-label"))
                .unwrap()
                .trim_end(),
            expected
        );
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        let claim = calls
            .lines()
            .position(|line| line.contains("agent get"))
            .unwrap();
        let rename = calls
            .lines()
            .position(|line| {
                line.ends_with(&format!("agent prompt {agent_name} /rename {expected}"))
            })
            .expect("native rename");
        let label = calls
            .lines()
            .position(|line| line.ends_with(&format!("pane rename w7:p1 {expected}")))
            .expect("Herdr label");
        let label_read = calls
            .lines()
            .position(|line| line.ends_with("pane get w7:p1"))
            .expect("Herdr label readback");
        assert!(claim < rename && rename < label && label < label_read);
    }

    #[test]
    fn claude_title_readback_that_differs_refuses_before_the_label() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let mut launch = launch(HarnessKind::Claude);
        launch.launch_profile.address.aspect = Aspect::Psyche;
        launch.launch_profile.model_name = "claude-fable-5-1".into();
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        // The pane keeps the title another Flow left behind.
        fs::write(
            root.path().join("native-title-override"),
            "Psyche Opus b87854",
        )
        .unwrap();
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        adapter.start_native_harness(&launch, &pane, None).unwrap();
        let binding = adapter
            .observe_native_binding(&launch, &pane, None)
            .unwrap();
        assert_eq!(
            adapter.title_native_flow(&launch, &binding).unwrap_err(),
            TitleRefusal::ReadbackDiffers
        );
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        assert!(!calls.contains("pane rename"));
    }

    /// A resumed session whose latest title record is already its title is
    /// not renamed again: nothing is typed, and its new pane is labelled.
    #[test]
    fn a_session_already_carrying_its_title_is_not_renamed() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let expected = "{ Field flow Secondary 123456 }";
        fs::write(
            root.path()
                .join("native-transcripts/claude")
                .join(format!("{native_session}.jsonl")),
            format!(
                "{}\n{}\n",
                serde_json::json!({"type":"custom-title","customTitle":"an older name","sessionId":native_session}),
                serde_json::json!({"type":"custom-title","customTitle":expected,"sessionId":native_session}),
            ),
        )
        .unwrap();
        let pane = adapter.create_launch_pane(&launch).unwrap();
        adapter.start_native_harness(&launch, &pane, None).unwrap();
        let binding = adapter
            .observe_native_binding(&launch, &pane, None)
            .unwrap();
        let title = adapter
            .title_native_flow(&launch, &binding)
            .expect("the kept title stands");
        assert_eq!(title.as_str(), expected);
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        assert!(!calls.contains("/rename"), "{calls}");
        assert_eq!(
            fs::read_to_string(root.path().join("pane-label"))
                .unwrap()
                .trim_end(),
            expected
        );
    }

    /// A typed `/rename` is read back only from a title record written after
    /// it: the terminal title alone, or a record from before, is not enough.
    #[test]
    fn a_typed_rename_waits_for_its_own_title_record() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let transcript = root
            .path()
            .join("native-transcripts/claude")
            .join(format!("{native_session}.jsonl"));
        fs::write(
            &transcript,
            format!(
                "{}\n",
                serde_json::json!({"type":"custom-title","customTitle":"an older name","sessionId":native_session})
            ),
        )
        .unwrap();
        // Claude writes the record only some reads after the command.
        fs::write(root.path().join("title-record-delay"), "5").unwrap();
        let pane = adapter.create_launch_pane(&launch).unwrap();
        adapter.start_native_harness(&launch, &pane, None).unwrap();
        let binding = adapter
            .observe_native_binding(&launch, &pane, None)
            .unwrap();
        let title = adapter
            .title_native_flow(&launch, &binding)
            .expect("read back once its own record is written");
        assert!(!root.path().join("title-record-pending").exists());
        assert_eq!(
            adapter
                .claude_transcript_titles(&pane, native_session)
                .unwrap(),
            vec!["an older name".to_owned(), title.as_str().to_owned()]
        );

        // A record that never comes refuses, though the terminal shows it.
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        fs::write(root.path().join("title-record-delay"), "100000").unwrap();
        let pane = adapter.create_launch_pane(&launch).unwrap();
        adapter.start_native_harness(&launch, &pane, None).unwrap();
        let binding = adapter
            .observe_native_binding(&launch, &pane, None)
            .unwrap();
        assert_eq!(
            adapter.title_native_flow(&launch, &binding).unwrap_err(),
            TitleRefusal::ReadbackDiffers
        );
        assert!(!root.path().join("pane-label").exists());
    }

    #[test]
    fn a_model_alias_is_titled_from_the_address() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let mut launch = launch(HarnessKind::Claude);
        launch.launch_profile.model_name = "haiku".into();
        launch.launch_profile.address.layer = signal_flow::Layer::Quaternary;
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).unwrap();
        adapter.start_native_harness(&launch, &pane, None).unwrap();
        let binding = adapter
            .observe_native_binding(&launch, &pane, None)
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
    fn claude_readiness_is_waited_for_before_the_title() {
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
        adapter.start_native_harness(&launch, &pane, None).unwrap();
        let binding = adapter
            .observe_native_binding(&launch, &pane, None)
            .unwrap();
        // Just bound: Herdr does not show the official session yet; it does
        // once the harness comes to rest.
        fs::write(root.path().join("missing-agent-session"), "absent").unwrap();
        let title = adapter
            .title_native_flow(&launch, &binding)
            .expect("title set once the target is ready");
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        let wait = calls
            .lines()
            .position(|line| {
                line.ends_with("agent wait w7:p1 --until idle --until done --timeout 1000")
            })
            .expect("readiness wait");
        let rename = calls
            .lines()
            .position(|line| line.contains(&format!("/rename {}", title.as_str())))
            .expect("native rename");
        assert!(wait < rename);
    }

    #[test]
    fn a_target_that_never_comes_to_rest_is_refused_typed_before_any_rename() {
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
        adapter.start_native_harness(&launch, &pane, None).unwrap();
        let binding = adapter
            .observe_native_binding(&launch, &pane, None)
            .unwrap();
        fs::write(root.path().join("agent-wait-refused"), "timeout").unwrap();
        assert_eq!(
            adapter.title_native_flow(&launch, &binding).unwrap_err(),
            TitleRefusal::NotReady
        );
        // The wait answers, but Herdr shows another native session there.
        fs::remove_file(root.path().join("agent-wait-refused")).unwrap();
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
        adapter.start_native_harness(&launch, &pane, None).unwrap();
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
        adapter.start_native_harness(&launch, &pane, None).unwrap();
        fs::write(root.path().join("agent-wait-refused"), "timeout").unwrap();
        let error = adapter
            .observe_native_binding(&launch, &pane, None)
            .unwrap_err();
        assert!(error.contains("not ready"), "{error}");
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        assert!(!calls.contains("agent get"));
    }

    fn promptable(
        root: &tempfile::TempDir,
        adapter: &HerdrCli,
        launch: &ComposedLaunch,
        native_session: &str,
    ) -> PromptDeliveryIntent {
        let _ = root;
        let pane = adapter.create_launch_pane(launch).unwrap();
        adapter.start_native_harness(launch, &pane, None).unwrap();
        let binding = adapter.observe_native_binding(launch, &pane, None).unwrap();
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
    fn the_first_prompt_waits_through_herdr_after_the_rename() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let intent = promptable(&root, &adapter, &launch, native_session);
        let calls_before = fs::read_to_string(root.path().join("calls")).unwrap();
        let rename = calls_before
            .lines()
            .position(|line| line.contains("/rename"))
            .expect("rename");
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
        assert!(rename < wait, "{calls}");
        assert!(
            wait >= calls_before.lines().count(),
            "the wait follows the rename"
        );
        assert_eq!(socket_prompts(&root).len(), 1);
    }

    #[test]
    fn a_first_prompt_whose_composer_never_rests_is_refused_typed_and_nothing_is_typed() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = launch.launch_agent_name();
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let intent = promptable(&root, &adapter, &launch, native_session);
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
        adapter.codex_endpoints.stable.client_path = proxy;
        adapter
            .codex_endpoints
            .stable
            .model_names
            .insert("gpt-6-astra".into());
        adapter
            .codex_endpoints
            .next
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
}
