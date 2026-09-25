//! Staged native launch through Herdr's documented CLI surface.
//!
//! Launching, claiming an identity, registering it, submitting the first
//! prompt, and observing its target-side receipt are separate operations.
//! The adapter never chooses registration policy and never retries a prompt
//! whose terminal write may already have succeeded.

use super::{DecodesFlowClaim, FlowClaim, HerdrCli};
use crate::codex::NamesBoundCodexThread;
use crate::composition::{
    ClaudeCommandStack, LaunchReceipt, NamesRemoteControl, ValidatesComposedPrompt,
};
use crate::title::NativeTitle;
use sha2::{Digest, Sha256};
use signal_flow::{
    ComposedLaunch, HarnessKind, HerdrPaneBinding, NativeLaunchBinding, NativeSkillSelection,
    NativeTargetReceipt, NativeTranscriptAbsence, NativeTranscriptBoundary, NativeTranscriptCursor,
    PromptDeliveryIntent, PromptDeliveryResult, RegistrationAcknowledgement,
};
use std::{
    fs::{self, File, Metadata},
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

/// Creates topology without launching a harness or sending a prompt.
pub trait CreatesHerdrLaunchPane {
    fn create_launch_pane(&self, launch: &ComposedLaunch) -> Result<HerdrPaneBinding, String>;
}

/// Starts the selected native interactive harness without a positional prompt.
pub trait StartsNativeHerdrHarness {
    fn start_native_harness(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
    ) -> Result<(), String>;
}

/// Observes an official Herdr integration report and binds it to an exact
/// native identity claim. Screen detection and caller metadata are rejected.
pub trait ObservesNativeLaunchBinding {
    fn observe_native_binding(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
    ) -> Result<NativeLaunchBinding, String>;
}

/// After the Flow ID is claimed, sets the canonical native title and the
/// Herdr pane label, and reads both back before any prompt is authorized.
pub trait TitlesNativeFlow {
    fn title_native_flow(
        &self,
        launch: &ComposedLaunch,
        binding: &NativeLaunchBinding,
    ) -> Result<NativeTitle, String>;
}

/// Converts an exact registration acknowledgement into the value that the
/// integration layer must durably store before prompt submission.
pub trait AcceptsLaunchRegistration {
    fn accept_registration(
        &self,
        launch: &ComposedLaunch,
        binding: &NativeLaunchBinding,
        acknowledgement: &RegistrationAcknowledgement,
        native_skill_selection_vector: Vec<NativeSkillSelection>,
    ) -> Result<PromptDeliveryIntent, String>;
}

/// Resolves Claude skills with its documented enterprise, personal, then
/// project catalog precedence before any prompt can be durably authorized.
pub trait ResolvesClaudeNativeSkills {
    fn resolve_claude_native_skills(
        &self,
        launch: &ComposedLaunch,
        binding: &NativeLaunchBinding,
    ) -> Result<Vec<NativeSkillSelection>, String>;
}

/// Performs one prompt submission attempt per invocation. The caller must
/// durably gate invocation; a successful Herdr write remains ambiguous until
/// the native transcript independently contains the requested receipt.
pub trait SubmitsFirstPromptOnce {
    fn submit_first_prompt_once(
        &self,
        launch: &ComposedLaunch,
        durable_intent: &PromptDeliveryIntent,
    ) -> Result<PromptDeliveryResult, String>;
}

/// Promotes an ambiguous delivery only from an exact assistant record in the
/// native transcript for the bound session and turn. If a transcript was
/// absent at submission and later appears without a receipt, the returned
/// ambiguous intent contains its adopted cursor and must replace the stored
/// absence boundary before another observation.
pub trait ObservesNativeTargetReceipt {
    fn observe_native_target_receipt(
        &self,
        durable_intent: &PromptDeliveryIntent,
    ) -> Result<PromptDeliveryResult, String>;
}

impl HerdrCli {
    const CLAUDE_CHILD_SESSION_ENVIRONMENT: &'static str = "CLAUDE_CODE_CHILD_SESSION";
    /// Inherited Claude identity that a new Flow must not carry. A shared
    /// `CLAUDE_JOB_DIR` shares one job state between processes: a new
    /// session adopts the title another session left there, and its own
    /// `/rename` is propagated to every process on that job directory.
    const CLAUDE_INHERITED_ENVIRONMENT: [&'static str; 4] = [
        Self::CLAUDE_CHILD_SESSION_ENVIRONMENT,
        "CLAUDE_JOB_DIR",
        "CLAUDE_CODE_SESSION_ID",
        "CLAUDE_CODE_SESSION_KIND",
    ];
    const CLAUDE_SKIP_PERMISSIONS_FLAG: &'static str = "--dangerously-skip-permissions";
    /// Claude offers "Make auto mode your default permission mode?" when the
    /// user settings name a non-auto default mode and no other settings
    /// source names one. Flag settings are such a source: naming the mode the
    /// launch already runs in suppresses the offer without touching any
    /// settings file.
    const CLAUDE_FLAG_SETTINGS: &'static str =
        r#"{"permissions":{"defaultMode":"bypassPermissions"}}"#;
    const CLAUDE_REMOTE_CONTROL_FLAG: &'static str = "--remote-control";

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

    fn claude_environment_preparation(marker_suffix: &str) -> String {
        format!(
            "unset {} && printf 'FLOW_CLAUDE_ENV_READY_%s\\n' {}",
            Self::CLAUDE_INHERITED_ENVIRONMENT.join(" "),
            marker_suffix
        )
    }

    fn prepare_claude_pane_environment(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
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
            Self::claude_environment_preparation(&marker_suffix),
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
            "visible".into(),
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

    fn expected_harness(harness: &HarnessKind) -> &'static str {
        match harness {
            HarnessKind::Claude => "claude",
            HarnessKind::Codex => "codex",
        }
    }

    fn launch_agent_name(launch: &ComposedLaunch) -> String {
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
            Self::expected_harness(&launch.launch_profile.harness_kind),
            &hash[..24]
        )
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

    fn sha256_file(path: &Path) -> Result<String, String> {
        let metadata = fs::symlink_metadata(path)
            .map_err(|error| format!("native skill source metadata failed: {error}"))?;
        if !path.is_absolute() || metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err("native skill source is not an absolute regular file".into());
        }
        let source = fs::read(path)
            .map_err(|error| format!("native skill source is unreadable: {error}"))?;
        Ok(format!("{:x}", Sha256::digest(source)))
    }

    fn validate_skill_selections(
        launch: &ComposedLaunch,
        selections: &[NativeSkillSelection],
    ) -> Result<(), String> {
        if selections
            .iter()
            .map(|selection| selection.skill_name.as_str())
            .ne(launch
                .launch_profile
                .skill_name_vector
                .iter()
                .map(String::as_str))
        {
            return Err("native skill selection order differs from launch profile".into());
        }
        for selection in selections {
            let path = Path::new(&selection.native_skill_path);
            if Self::sha256_file(path)? != selection.native_skill_sha256 {
                return Err(format!(
                    "native skill source changed for {}",
                    selection.skill_name
                ));
            }
        }
        Ok(())
    }

    fn binding_matches_launch(
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
    ) -> Result<(), String> {
        if pane.launch_request_id != launch.launch_profile.launch_request_id
            || pane.herdr_session_name != launch.launch_profile.herdr_session_name
            || pane.herdr_agent_name != Self::launch_agent_name(launch)
        {
            return Err("Herdr pane binding does not belong to this launch request".into());
        }
        Ok(())
    }

    fn prompt_intent_matches_launch(
        launch: &ComposedLaunch,
        intent: &PromptDeliveryIntent,
    ) -> Result<(), String> {
        if !launch.has_canonical_first_prompt() {
            return Err("composed first prompt is not canonical".into());
        }
        Self::binding_matches_launch(launch, &intent.herdr_pane_binding)?;
        if intent.launch_request_id != launch.launch_profile.launch_request_id
            || intent.prompt_sha256 != launch.first_prompt_payload.prompt_sha256
            || intent.harness_kind != launch.launch_profile.harness_kind
            || intent.model_name != launch.launch_profile.model_name
            || intent.effort != launch.launch_profile.effort
            || !Self::boundary_matches_intent(intent)
        {
            return Err("durable prompt intent does not belong to this composed launch".into());
        }
        Self::validate_skill_selections(launch, &intent.native_skill_selection_vector)?;
        Ok(())
    }

    fn boundary_matches_intent(intent: &PromptDeliveryIntent) -> bool {
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
        let harness_name = Self::expected_harness(harness);
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
    ) -> Result<serde_json::Value, String> {
        let response = self.run_json(&[
            "--session".into(),
            pane.herdr_session_name.clone(),
            "agent".into(),
            "get".into(),
            pane.herdr_agent_name.clone(),
        ])?;
        let agent = response
            .pointer("/result/agent")
            .ok_or_else(|| "Herdr agent get returned no receipt target".to_owned())?;
        if agent.get("name").and_then(serde_json::Value::as_str)
            != Some(pane.herdr_agent_name.as_str())
            || agent
                .get("workspace_id")
                .and_then(serde_json::Value::as_str)
                != Some(pane.herdr_workspace_id.as_str())
            || agent.get("pane_id").and_then(serde_json::Value::as_str)
                != Some(pane.herdr_pane_id.as_str())
            || agent.get("terminal_id").and_then(serde_json::Value::as_str)
                != Some(pane.herdr_terminal_id.as_str())
        {
            return Err("receipt target no longer matches the registered Herdr pane".into());
        }
        let session = agent
            .get("agent_session")
            .ok_or_else(|| "receipt target has no official native session".to_owned())?;
        let expected_harness = Self::expected_harness(harness);
        if agent.get("agent").and_then(serde_json::Value::as_str) != Some(expected_harness)
            || session.get("source").and_then(serde_json::Value::as_str)
                != Some(format!("herdr:{expected_harness}").as_str())
            || session.get("agent").and_then(serde_json::Value::as_str) != Some(expected_harness)
            || session.get("kind").and_then(serde_json::Value::as_str) != Some("id")
            || session.get("value").and_then(serde_json::Value::as_str) != Some(native_session_id)
        {
            return Err("receipt target native identity is no longer exact".into());
        }
        Ok(agent.clone())
    }

    fn collect_native_transcripts(
        directory: &Path,
        native_session_id: &str,
        harness: &HarnessKind,
        depth: usize,
        found: &mut Vec<PathBuf>,
    ) -> Result<(), String> {
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
                Self::collect_native_transcripts(
                    &path,
                    native_session_id,
                    harness,
                    depth + 1,
                    found,
                )?;
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
        Self::collect_native_transcripts(
            &canonical_root,
            native_session_id,
            harness,
            0,
            &mut found,
        )?;
        let root_after = fs::metadata(&canonical_root)
            .map_err(|error| format!("native transcript root metadata failed: {error}"))?;
        if root_before.dev() != root_after.dev() || root_before.ino() != root_after.ino() {
            return Err("configured native transcript root changed during observation".into());
        }
        Ok((canonical_root, root_after, found))
    }

    fn one_resolved_transcript(root: &Path, found: Vec<PathBuf>) -> Result<PathBuf, String> {
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

    fn hash_prefix(file: &mut File, length: u64) -> Result<(String, Option<u8>), String> {
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
        let transcript = Self::one_resolved_transcript(&root, found)?;
        let mut file = File::open(&transcript)
            .map_err(|error| format!("native transcript is unreadable: {error}"))?;
        let metadata = file
            .metadata()
            .map_err(|error| format!("native transcript metadata failed: {error}"))?;
        let byte_offset = metadata.len();
        let (prefix_sha256, last_byte) = Self::hash_prefix(&mut file, byte_offset)?;
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
        let transcript = Self::one_resolved_transcript(&root, found)?;
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

    fn decimal_identity(value: &str, label: &str) -> Result<u64, String> {
        let parsed = value
            .parse::<u64>()
            .map_err(|_| format!("native transcript {label} is invalid"))?;
        if parsed.to_string() != value {
            return Err(format!("native transcript {label} is not canonical"));
        }
        Ok(parsed)
    }

    fn receipt_input(&self, intent: &PromptDeliveryIntent) -> Result<Option<(File, bool)>, String> {
        if !Self::boundary_matches_intent(intent) {
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
                let expected_device =
                    Self::decimal_identity(&absence.transcript_root_device, "root device")?;
                let expected_inode =
                    Self::decimal_identity(&absence.transcript_root_inode, "root inode")?;
                if root_metadata.dev() != expected_device || root_metadata.ino() != expected_inode {
                    return Err("configured native transcript root was replaced".into());
                }
                if found.is_empty() {
                    return Ok(None);
                }
                let transcript = Self::one_resolved_transcript(&root, found)?;
                File::open(transcript)
                    .map(|file| Some((file, true)))
                    .map_err(|error| format!("native transcript is unreadable: {error}"))
            }
            NativeTranscriptBoundary::Existing(cursor) => {
                let transcript = Self::one_resolved_transcript(&root, found)?;
                let mut file = File::open(transcript)
                    .map_err(|error| format!("native transcript is unreadable: {error}"))?;
                let metadata = file
                    .metadata()
                    .map_err(|error| format!("native transcript metadata failed: {error}"))?;
                let expected_device = Self::decimal_identity(&cursor.transcript_device, "device")?;
                let expected_inode = Self::decimal_identity(&cursor.transcript_inode, "inode")?;
                let byte_offset = u64::try_from(cursor.transcript_byte_offset)
                    .map_err(|_| "native transcript boundary is negative".to_owned())?;
                if metadata.dev() != expected_device || metadata.ino() != expected_inode {
                    return Err("native transcript file was replaced".into());
                }
                if metadata.len() < byte_offset {
                    return Err("native transcript file was truncated".into());
                }
                let (prefix_sha256, _) = Self::hash_prefix(&mut file, byte_offset)?;
                if prefix_sha256 != cursor.transcript_prefix_sha256 {
                    return Err("native transcript prefix changed after intent persistence".into());
                }
                file.seek(SeekFrom::Start(byte_offset))
                    .map_err(|error| format!("native transcript seek failed: {error}"))?;
                Ok(Some((file, false)))
            }
        }
    }

    fn assistant_receipt(
        row: &serde_json::Value,
        native_session_id: &str,
        expected: &str,
    ) -> Option<String> {
        if row.get("type").and_then(serde_json::Value::as_str) == Some("event_msg")
            && row
                .pointer("/payload/thread_id")
                .and_then(serde_json::Value::as_str)
                == Some(native_session_id)
            && row
                .pointer("/payload/item/type")
                .and_then(serde_json::Value::as_str)
                == Some("AgentMessage")
        {
            let turn = row
                .pointer("/payload/turn_id")
                .and_then(serde_json::Value::as_str)?;
            let contents = row
                .pointer("/payload/item/content")
                .and_then(serde_json::Value::as_array)?;
            return (contents.len() == 1
                && contents[0].get("text").and_then(serde_json::Value::as_str) == Some(expected))
            .then(|| turn.to_owned());
        }

        if row.get("type").and_then(serde_json::Value::as_str) == Some("assistant")
            && row
                .get("sessionId")
                .or_else(|| row.get("session_id"))
                .and_then(serde_json::Value::as_str)
                == Some(native_session_id)
        {
            let turn = row.get("uuid").and_then(serde_json::Value::as_str)?;
            let contents = row
                .pointer("/message/content")
                .and_then(serde_json::Value::as_array)?;
            return (contents.len() == 1
                && contents[0].get("text").and_then(serde_json::Value::as_str) == Some(expected))
            .then(|| turn.to_owned());
        }

        None
    }

    fn skill_source(selection: &NativeSkillSelection) -> Result<String, String> {
        let path = Path::new(&selection.native_skill_path);
        if Self::sha256_file(path)? != selection.native_skill_sha256 {
            return Err(format!(
                "native skill source changed for {}",
                selection.skill_name
            ));
        }
        fs::read_to_string(path).map_err(|error| {
            format!(
                "native skill source is not UTF-8 for {}: {error}",
                selection.skill_name
            )
        })
    }

    fn claude_skill_expansion(selection: &NativeSkillSelection) -> Result<String, String> {
        let source = Self::skill_source(selection)?;
        let body = if let Some(after_open) = source.strip_prefix("---\n") {
            let closing = after_open.find("\n---\n").ok_or_else(|| {
                format!(
                    "native Claude skill {} has unterminated frontmatter",
                    selection.skill_name
                )
            })?;
            after_open[closing + "\n---\n".len()..].trim_start_matches('\n')
        } else {
            source.as_str()
        };
        let directory = Path::new(&selection.native_skill_path)
            .parent()
            .ok_or_else(|| "native Claude skill has no base directory".to_owned())?;
        Ok(format!(
            "Base directory for this skill: {}\n\n{body}",
            directory.display()
        ))
    }

    fn codex_skill_expansion(selection: &NativeSkillSelection) -> Result<String, String> {
        let source = Self::skill_source(selection)?;
        Ok(format!(
            "<skill>\n<name>{}</name>\n<path>{}</path>\n{}\n</skill>",
            selection.skill_name, selection.native_skill_path, source
        ))
    }

    fn prompt_text_matches_intent(text: &str, intent: &PromptDeliveryIntent) -> bool {
        text.strip_suffix(&LaunchReceipt::footer())
            .is_some_and(|body| {
                format!("{:x}", Sha256::digest(body.as_bytes())) == intent.prompt_sha256
            })
    }

    /// Reads one command record of a Claude user turn: the harness records
    /// each head command it loads as its name and the argument that follows
    /// the whole command stack.
    fn claude_command_record(text: &str) -> Option<(String, String)> {
        let rest = text.strip_prefix("<command-message>")?;
        let (message, rest) = rest.split_once("</command-message>\n<command-name>/")?;
        let (name, rest) = rest.split_once("</command-name>")?;
        if message != name {
            return None;
        }
        let argument = if rest.is_empty() {
            ""
        } else {
            rest.strip_prefix("\n<command-args>")?
                .strip_suffix("</command-args>")?
        };
        Some((name.to_owned(), argument.to_owned()))
    }

    /// The text that was typed when `names` were stacked at the head of a
    /// block whose remaining text is `argument`.
    fn claude_stacked_typed_text(names: &[&str], argument: &str) -> String {
        let mut typed = names
            .iter()
            .map(|name| format!("/{name}"))
            .collect::<Vec<_>>()
            .join(" ");
        if !argument.is_empty() {
            typed.push(' ');
            typed.push_str(argument);
        }
        typed
    }
}

impl HerdrCli {
    const TITLE_READBACK_ATTEMPTS: usize = 40;
    #[cfg(not(test))]
    const TITLE_READBACK_INTERVAL: Duration = Duration::from_millis(250);
    #[cfg(test)]
    const TITLE_READBACK_INTERVAL: Duration = Duration::from_millis(1);

    /// The last title the native Claude transcript records for its session,
    /// or none while no transcript or no title record exists.
    fn claude_transcript_title(
        &self,
        pane: &HerdrPaneBinding,
        native_session_id: &str,
    ) -> Result<Option<String>, String> {
        let (root, _, found) =
            self.transcript_candidates(pane, native_session_id, &HarnessKind::Claude, None)?;
        if found.is_empty() {
            return Ok(None);
        }
        let transcript = fs::read(Self::one_resolved_transcript(&root, found)?)
            .map_err(|error| format!("native transcript is unreadable: {error}"))?;
        Ok(transcript
            .split(|byte| *byte == b'\n')
            .rev()
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
            .next())
    }

    /// Renames the Claude session with its own `/rename` command and reads
    /// the title back from the terminal title Claude sets and from the
    /// session's transcript title record once one exists.
    fn title_claude_session(
        &self,
        binding: &NativeLaunchBinding,
        title: &NativeTitle,
    ) -> Result<(), String> {
        let pane = &binding.herdr_pane_binding;
        let agent =
            self.verify_native_target(pane, &binding.native_session_id, &HarnessKind::Claude)?;
        if agent
            .get("interactive_ready")
            .and_then(serde_json::Value::as_bool)
            != Some(true)
        {
            return Err("Claude target is not interactively ready for its title".into());
        }
        self.run_json(&[
            "--session".into(),
            pane.herdr_session_name.clone(),
            "agent".into(),
            "prompt".into(),
            pane.herdr_agent_name.clone(),
            format!("/rename {}", title.as_str()),
        ])?;
        for _ in 0..Self::TITLE_READBACK_ATTEMPTS {
            let agent =
                self.verify_native_target(pane, &binding.native_session_id, &HarnessKind::Claude)?;
            let terminal = agent
                .get("terminal_title_stripped")
                .and_then(serde_json::Value::as_str);
            let recorded = self.claude_transcript_title(pane, &binding.native_session_id)?;
            if terminal == Some(title.as_str())
                && recorded
                    .as_deref()
                    .is_none_or(|name| name == title.as_str())
            {
                return Ok(());
            }
            std::thread::sleep(Self::TITLE_READBACK_INTERVAL);
        }
        Err("Claude native title readback differs from the set title".into())
    }

    /// Labels the launch pane with the title and reads the label back.
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
    ) -> Result<NativeTitle, String> {
        Self::binding_matches_launch(launch, &binding.herdr_pane_binding)?;
        if binding.launch_request_id != launch.launch_profile.launch_request_id
            || binding.harness_kind != launch.launch_profile.harness_kind
        {
            return Err("native binding does not belong to this launch".into());
        }
        let title = NativeTitle::for_flow(&launch.launch_profile, &binding.flow_id)
            .map_err(|refusal| refusal.to_string())?;
        match binding.harness_kind {
            HarnessKind::Claude => self.title_claude_session(binding, &title)?,
            HarnessKind::Codex => self
                .codex_endpoints
                .adapter_for(&launch.launch_profile.model_name)
                .and_then(|adapter| {
                    adapter.name_bound_thread(&binding.native_session_id, title.as_str())
                })
                .map_err(|error| error.to_string())?,
        }
        self.label_herdr_pane(&binding.herdr_pane_binding, &title)?;
        Ok(title)
    }
}

impl CreatesHerdrLaunchPane for HerdrCli {
    fn create_launch_pane(&self, launch: &ComposedLaunch) -> Result<HerdrPaneBinding, String> {
        let agent_name = Self::launch_agent_name(launch);
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
    ) -> Result<(), String> {
        Self::binding_matches_launch(launch, pane)?;
        if launch.launch_profile.harness_kind == HarnessKind::Claude {
            self.prepare_claude_pane_environment(launch, pane)?;
        }
        let harness = Self::expected_harness(&launch.launch_profile.harness_kind);
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
            arguments.push(Self::CLAUDE_SKIP_PERMISSIONS_FLAG.into());
            arguments.push("--settings".into());
            arguments.push(Self::CLAUDE_FLAG_SETTINGS.into());
            // Every Claude Flow is remotely controllable; the name never
            // begins with `-`, so the optional value binds to the flag.
            arguments.push(Self::CLAUDE_REMOTE_CONTROL_FLAG.into());
            arguments.push(launch.launch_profile.remote_control_name());
            arguments.push("--system-prompt-file".into());
            arguments.push(launch.launch_profile.system_prompt_bundle_file.clone());
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
            }
        }
        // No positional prompt: the composed first prompt is the only one.
        self.run_json(&arguments).map(|_| ())
    }
}

impl ObservesNativeLaunchBinding for HerdrCli {
    fn observe_native_binding(
        &self,
        launch: &ComposedLaunch,
        pane: &HerdrPaneBinding,
    ) -> Result<NativeLaunchBinding, String> {
        Self::binding_matches_launch(launch, pane)?;
        let expected_harness = Self::expected_harness(&launch.launch_profile.harness_kind);
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
        let session = agent
            .get("agent_session")
            .ok_or_else(|| "official Herdr integration reported no native session".to_owned())?;
        let expected_source = format!("herdr:{expected_harness}");
        if session.get("source").and_then(serde_json::Value::as_str)
            != Some(expected_source.as_str())
            || session.get("agent").and_then(serde_json::Value::as_str) != Some(expected_harness)
            || session.get("kind").and_then(serde_json::Value::as_str) != Some("id")
        {
            return Err("native identity did not come from the official Herdr integration".into());
        }
        let native_session_id = session
            .get("value")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                "official Herdr integration returned an empty native identity".to_owned()
            })?;
        let normalized_identity = native_session_id
            .bytes()
            .filter(|byte| *byte != b'-')
            .map(char::from)
            .collect::<String>()
            .to_ascii_lowercase();
        let flow_id =
            self.claim_flow_identity(&launch.launch_profile.harness_kind, native_session_id)?;
        if self.claimed_native_identity(&flow_id, &launch.launch_profile.harness_kind)?
            != normalized_identity
        {
            return Err("flow claim does not contain the observed native identity".into());
        }
        Ok(NativeLaunchBinding {
            launch_request_id: launch.launch_profile.launch_request_id.clone(),
            flow_id,
            native_session_id: native_session_id.into(),
            harness_kind: launch.launch_profile.harness_kind.clone(),
            herdr_pane_binding: pane.clone(),
        })
    }
}

impl ResolvesClaudeNativeSkills for HerdrCli {
    fn resolve_claude_native_skills(
        &self,
        launch: &ComposedLaunch,
        binding: &NativeLaunchBinding,
    ) -> Result<Vec<NativeSkillSelection>, String> {
        if launch.launch_profile.harness_kind != HarnessKind::Claude
            || binding.harness_kind != HarnessKind::Claude
            || binding.launch_request_id != launch.launch_profile.launch_request_id
        {
            return Err("Claude skill resolution does not match its native binding".into());
        }
        let mut roots = Vec::new();
        for configured in &self.claude_skill_roots {
            if !configured.exists() {
                continue;
            }
            let canonical = configured.canonicalize().map_err(|error| {
                format!("configured Claude skill catalog is unavailable: {error}")
            })?;
            if !canonical.is_dir() {
                return Err("configured Claude skill catalog is not a directory".into());
            }
            roots.push(canonical);
        }
        launch
            .launch_profile
            .skill_name_vector
            .iter()
            .map(|name| {
                let mut selected = None;
                for root in &roots {
                    let candidate = root.join(name).join("SKILL.md");
                    if !candidate.exists() {
                        continue;
                    }
                    let canonical = candidate.canonicalize().map_err(|error| {
                        format!("native Claude skill {name} cannot be canonicalized: {error}")
                    })?;
                    if !canonical.starts_with(root) {
                        return Err(format!(
                            "native Claude skill {name} resolves outside its catalog"
                        ));
                    }
                    selected = Some(canonical);
                    break;
                }
                let path = selected.ok_or_else(|| {
                    format!("required native Claude skill is unavailable: {name}")
                })?;
                Ok(NativeSkillSelection {
                    skill_name: name.clone(),
                    native_skill_sha256: Self::sha256_file(&path)?,
                    native_skill_path: path.to_string_lossy().into_owned(),
                })
            })
            .collect()
    }
}

impl AcceptsLaunchRegistration for HerdrCli {
    fn accept_registration(
        &self,
        launch: &ComposedLaunch,
        binding: &NativeLaunchBinding,
        acknowledgement: &RegistrationAcknowledgement,
        native_skill_selection_vector: Vec<NativeSkillSelection>,
    ) -> Result<PromptDeliveryIntent, String> {
        Self::binding_matches_launch(launch, &binding.herdr_pane_binding)?;
        if binding.harness_kind != launch.launch_profile.harness_kind
            || acknowledgement.launch_request_id != binding.launch_request_id
            || acknowledgement.flow_id != binding.flow_id
            || acknowledgement.native_session_id != binding.native_session_id
            || acknowledgement.herdr_pane_binding != binding.herdr_pane_binding
        {
            return Err("registration acknowledgement does not match native binding".into());
        }
        Self::validate_skill_selections(launch, &native_skill_selection_vector)?;
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
            native_skill_selection_vector,
            native_transcript_boundary,
        })
    }
}

impl SubmitsFirstPromptOnce for HerdrCli {
    fn submit_first_prompt_once(
        &self,
        launch: &ComposedLaunch,
        durable_intent: &PromptDeliveryIntent,
    ) -> Result<PromptDeliveryResult, String> {
        Self::prompt_intent_matches_launch(launch, durable_intent)?;
        if durable_intent.harness_kind != HarnessKind::Claude {
            return Err("Codex first turns require the bound native typed-skill controller".into());
        }
        self.run_json(&[
            "--session".into(),
            durable_intent.herdr_pane_binding.herdr_session_name.clone(),
            "agent".into(),
            "prompt".into(),
            durable_intent.herdr_pane_binding.herdr_agent_name.clone(),
            launch.first_prompt_payload.first_prompt_text.clone(),
        ])?;
        Ok(PromptDeliveryResult::Ambiguous(durable_intent.clone()))
    }
}

impl ObservesNativeTargetReceipt for HerdrCli {
    fn observe_native_target_receipt(
        &self,
        durable_intent: &PromptDeliveryIntent,
    ) -> Result<PromptDeliveryResult, String> {
        let expected = LaunchReceipt::MARKER;
        let Some((input, adopted_after_absence)) = self.receipt_input(durable_intent)? else {
            return Ok(PromptDeliveryResult::Ambiguous(durable_intent.clone()));
        };
        let mut observed_turn = None;
        let mut native_turn = None;
        let mut input_verified = false;
        let mut skill_index = 0_usize;
        let mut pending_claude_tool = None;
        let mut claude_tool_succeeded = false;
        let mut claude_command_expansion_pending = false;
        let mut claude_commands_loaded = 0_usize;
        let mut claude_command_argument: Option<String> = None;
        let claude_stack =
            ClaudeCommandStack::stacked(durable_intent.native_skill_selection_vector.len());
        let mut reader = BufReader::new(input);
        loop {
            let mut record = Vec::new();
            let read = reader
                .read_until(b'\n', &mut record)
                .map_err(|error| format!("native transcript read failed: {error}"))?;
            if read == 0 {
                break;
            }
            if record.last() != Some(&b'\n') {
                break;
            }
            let Ok(row) = serde_json::from_slice::<serde_json::Value>(&record) else {
                continue;
            };
            match durable_intent.harness_kind {
                HarnessKind::Codex => {
                    if row.get("type").and_then(serde_json::Value::as_str) == Some("turn_context") {
                        let payload = row
                            .get("payload")
                            .ok_or_else(|| "native Codex turn context has no payload".to_owned())?;
                        if payload.get("model").and_then(serde_json::Value::as_str)
                            != Some(durable_intent.model_name.as_str())
                            || payload.get("effort").and_then(serde_json::Value::as_str)
                                != Some(durable_intent.effort.as_str())
                        {
                            return Err("native Codex model or effort differs from intent".into());
                        }
                        native_turn = payload
                            .get("turn_id")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned);
                    }
                    if row.get("type").and_then(serde_json::Value::as_str) == Some("event_msg")
                        && row
                            .pointer("/payload/thread_id")
                            .and_then(serde_json::Value::as_str)
                            == Some(durable_intent.native_session_id.as_str())
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
                            .ok_or_else(|| {
                                "native Codex first turn has no input vector".to_owned()
                            })?;
                        if turn != native_turn.as_deref()
                            || content.len()
                                != durable_intent.native_skill_selection_vector.len() + 1
                        {
                            return Err("native Codex first-turn input differs from intent".into());
                        }
                        for (got, want) in content
                            .iter()
                            .zip(&durable_intent.native_skill_selection_vector)
                        {
                            if got.get("type").and_then(serde_json::Value::as_str) != Some("skill")
                                || got.get("name").and_then(serde_json::Value::as_str)
                                    != Some(want.skill_name.as_str())
                                || got.get("path").and_then(serde_json::Value::as_str)
                                    != Some(want.native_skill_path.as_str())
                            {
                                return Err(
                                    "native Codex typed skill input differs from intent".into()
                                );
                            }
                        }
                        if content.last().and_then(|value| value.get("type"))
                            != Some(&serde_json::json!("text"))
                            || !content
                                .last()
                                .and_then(|value| value.get("text"))
                                .and_then(serde_json::Value::as_str)
                                .is_some_and(|text| {
                                    Self::prompt_text_matches_intent(text, durable_intent)
                                })
                        {
                            return Err("native Codex first-turn text differs from intent".into());
                        }
                        input_verified = true;
                    }
                    if input_verified
                        && row.get("type").and_then(serde_json::Value::as_str)
                            == Some("response_item")
                        && skill_index < durable_intent.native_skill_selection_vector.len()
                    {
                        let expected_expansion = Self::codex_skill_expansion(
                            &durable_intent.native_skill_selection_vector[skill_index],
                        )?;
                        let matched = row
                            .pointer("/payload/content")
                            .and_then(serde_json::Value::as_array)
                            .is_some_and(|content| {
                                content.iter().any(|item| {
                                    item.get("text").and_then(serde_json::Value::as_str)
                                        == Some(expected_expansion.as_str())
                                })
                            });
                        if matched {
                            skill_index += 1;
                        }
                    }
                }
                HarnessKind::Claude => {
                    let exact_session = row
                        .get("sessionId")
                        .or_else(|| row.get("session_id"))
                        .and_then(serde_json::Value::as_str)
                        == Some(durable_intent.native_session_id.as_str());
                    if exact_session
                        && row.get("type").and_then(serde_json::Value::as_str) == Some("assistant")
                        && let Some(contents) = row
                            .pointer("/message/content")
                            .and_then(serde_json::Value::as_array)
                    {
                        for content in contents {
                            if content.get("type").and_then(serde_json::Value::as_str)
                                != Some("tool_use")
                                || content.get("name").and_then(serde_json::Value::as_str)
                                    != Some("Skill")
                            {
                                continue;
                            }
                            if pending_claude_tool.is_some()
                                || claude_command_expansion_pending
                                || claude_commands_loaded > 0
                                    && claude_commands_loaded != claude_stack
                                || skill_index >= durable_intent.native_skill_selection_vector.len()
                                || content
                                    .pointer("/input/skill")
                                    .and_then(serde_json::Value::as_str)
                                    != Some(
                                        durable_intent.native_skill_selection_vector[skill_index]
                                            .skill_name
                                            .as_str(),
                                    )
                            {
                                return Err(
                                    "native Claude Skill invocation order differs from intent"
                                        .into(),
                                );
                            }
                            pending_claude_tool = content
                                .get("id")
                                .and_then(serde_json::Value::as_str)
                                .map(str::to_owned);
                            if pending_claude_tool.is_none() {
                                return Err("native Claude Skill invocation has no id".into());
                            }
                            claude_tool_succeeded = false;
                        }
                    }
                    if exact_session
                        && row.get("type").and_then(serde_json::Value::as_str) == Some("user")
                    {
                        if let Some(text) = row
                            .pointer("/message/content")
                            .and_then(serde_json::Value::as_str)
                        {
                            let names = durable_intent
                                .native_skill_selection_vector
                                .iter()
                                .take(claude_stack)
                                .map(|selection| selection.skill_name.as_str())
                                .collect::<Vec<_>>();
                            match Self::claude_command_record(text) {
                                Some((command, argument)) if claude_commands_loaded == 0 => {
                                    // The first command record carries the
                                    // argument after the whole stack; the
                                    // typed block is every stacked command
                                    // followed by it.
                                    let typed = Self::claude_stacked_typed_text(&names, &argument);
                                    if input_verified
                                        || names.first() != Some(&command.as_str())
                                        || !Self::prompt_text_matches_intent(&typed, durable_intent)
                                        || row
                                            .get("stackedOriginalInput")
                                            .and_then(serde_json::Value::as_str)
                                            .is_some_and(|original| original != typed)
                                    {
                                        return Err(
                                            "native Claude first-turn text differs from intent"
                                                .into(),
                                        );
                                    }
                                    claude_command_argument = Some(argument);
                                    claude_commands_loaded = 1;
                                    claude_command_expansion_pending = true;
                                    input_verified = true;
                                }
                                Some((command, argument)) => {
                                    if claude_command_expansion_pending
                                        || claude_commands_loaded >= claude_stack
                                        || names[claude_commands_loaded] != command
                                        || claude_command_argument.as_deref()
                                            != Some(argument.as_str())
                                    {
                                        return Err(
                                            "native Claude stacked command differs from intent"
                                                .into(),
                                        );
                                    }
                                    claude_commands_loaded += 1;
                                    claude_command_expansion_pending = true;
                                }
                                None => {
                                    if input_verified
                                        || !Self::prompt_text_matches_intent(text, durable_intent)
                                    {
                                        return Err(
                                            "native Claude first-turn text differs from intent"
                                                .into(),
                                        );
                                    }
                                    input_verified = true;
                                }
                            }
                        }
                        if let Some(tool_id) = pending_claude_tool.as_deref() {
                            let matching_result = row
                                .pointer("/message/content")
                                .and_then(serde_json::Value::as_array)
                                .and_then(|contents| {
                                    contents.iter().find(|content| {
                                        content.get("type").and_then(serde_json::Value::as_str)
                                            == Some("tool_result")
                                            && content
                                                .get("tool_use_id")
                                                .and_then(serde_json::Value::as_str)
                                                == Some(tool_id)
                                    })
                                });
                            if matching_result.is_some() {
                                let expected_name = &durable_intent.native_skill_selection_vector
                                    [skill_index]
                                    .skill_name;
                                if row.pointer("/toolUseResult/success")
                                    != Some(&serde_json::Value::Bool(true))
                                    || row
                                        .pointer("/toolUseResult/commandName")
                                        .and_then(serde_json::Value::as_str)
                                        != Some(expected_name.as_str())
                                {
                                    return Err("native Claude Skill tool reported failure".into());
                                }
                                claude_tool_succeeded = true;
                            }
                        }
                        let companion = row.get("isMeta").and_then(serde_json::Value::as_bool)
                            == Some(true)
                            && row
                                .get("turnCompanion")
                                .and_then(serde_json::Value::as_bool)
                                == Some(true);
                        if companion && claude_command_expansion_pending {
                            // The harness expands each stacked command
                            // itself, appending the argument after the body.
                            let expected_expansion = Self::claude_skill_expansion(
                                &durable_intent.native_skill_selection_vector
                                    [claude_commands_loaded - 1],
                            )?;
                            let contents = row
                                .pointer("/message/content")
                                .and_then(serde_json::Value::as_array)
                                .ok_or_else(|| {
                                    "native Claude command expansion has no content".to_owned()
                                })?;
                            if row
                                .get("sourceToolUseID")
                                .is_some_and(|value| !value.is_null())
                                || contents.len() != 1
                                || !contents[0]
                                    .get("text")
                                    .and_then(serde_json::Value::as_str)
                                    .is_some_and(|text| {
                                        text.starts_with(expected_expansion.trim_end())
                                    })
                            {
                                return Err(
                                    "native Claude command expansion differs from selection".into(),
                                );
                            }
                            skill_index = claude_commands_loaded;
                            claude_command_expansion_pending = false;
                        } else if companion {
                            let tool_id = pending_claude_tool.as_deref().ok_or_else(|| {
                                "native Claude skill expansion has no preceding Skill tool"
                                    .to_owned()
                            })?;
                            if !claude_tool_succeeded
                                || row
                                    .get("sourceToolUseID")
                                    .and_then(serde_json::Value::as_str)
                                    != Some(tool_id)
                            {
                                return Err(
                                    "native Claude skill expansion lacks a successful tool result"
                                        .into(),
                                );
                            }
                            let expected_expansion = Self::claude_skill_expansion(
                                &durable_intent.native_skill_selection_vector[skill_index],
                            )?;
                            let contents = row
                                .pointer("/message/content")
                                .and_then(serde_json::Value::as_array)
                                .ok_or_else(|| {
                                    "native Claude skill expansion has no content".to_owned()
                                })?;
                            if contents.len() != 1
                                || contents[0].get("text").and_then(serde_json::Value::as_str)
                                    != Some(expected_expansion.as_str())
                            {
                                return Err(
                                    "native Claude expanded skill source differs from selection"
                                        .into(),
                                );
                            }
                            skill_index += 1;
                            pending_claude_tool = None;
                            claude_tool_succeeded = false;
                        }
                    }
                }
            }
            if let Some(turn) =
                Self::assistant_receipt(&row, &durable_intent.native_session_id, expected)
            {
                if observed_turn.is_some() {
                    return Err("native transcript contains duplicate target receipts".into());
                }
                if skill_index != durable_intent.native_skill_selection_vector.len()
                    || matches!(durable_intent.harness_kind, HarnessKind::Codex)
                        && (!input_verified || native_turn.as_deref() != Some(turn.as_str()))
                    || matches!(durable_intent.harness_kind, HarnessKind::Claude)
                        && (!input_verified
                            || claude_command_expansion_pending
                            || claude_commands_loaded > 0 && claude_commands_loaded != claude_stack)
                {
                    return Err("target receipt preceded native skill confirmation".into());
                }
                if matches!(durable_intent.harness_kind, HarnessKind::Claude)
                    && (row
                        .pointer("/message/model")
                        .and_then(serde_json::Value::as_str)
                        != Some(durable_intent.model_name.as_str())
                        || row.get("effort").and_then(serde_json::Value::as_str)
                            != Some(durable_intent.effort.as_str()))
                {
                    return Err("native Claude model or effort differs from intent".into());
                }
                observed_turn = Some(turn);
            }
        }
        let Some(native_turn_id) = observed_turn else {
            if adopted_after_absence {
                let binding = NativeLaunchBinding {
                    launch_request_id: durable_intent.launch_request_id.clone(),
                    flow_id: durable_intent.flow_id.clone(),
                    native_session_id: durable_intent.native_session_id.clone(),
                    harness_kind: durable_intent.harness_kind.clone(),
                    herdr_pane_binding: durable_intent.herdr_pane_binding.clone(),
                };
                // Preserve the observed inode but rescan the first turn from
                // byte zero on reconciliation. Advancing to EOF here would
                // discard native input and skill evidence emitted before the
                // assistant receipt.
                let boundary =
                    self.capture_transcript_start_boundary(&binding, &durable_intent.model_name)?;
                if !matches!(boundary, NativeTranscriptBoundary::Existing(_)) {
                    return Err("new native transcript disappeared during adoption".into());
                }
                let mut updated_intent = durable_intent.clone();
                updated_intent.native_transcript_boundary = boundary;
                return Ok(PromptDeliveryResult::Ambiguous(updated_intent));
            }
            return Ok(PromptDeliveryResult::Ambiguous(durable_intent.clone()));
        };
        let receipt_sha256 = format!("{:x}", Sha256::digest(expected.as_bytes()));
        Ok(PromptDeliveryResult::Observed(NativeTargetReceipt {
            launch_request_id: durable_intent.launch_request_id.clone(),
            prompt_sha256: durable_intent.prompt_sha256.clone(),
            flow_id: durable_intent.flow_id.clone(),
            native_session_id: durable_intent.native_session_id.clone(),
            native_turn_id,
            receipt_sha256,
            model_name: durable_intent.model_name.clone(),
            effort: durable_intent.effort.clone(),
            native_skill_selection_vector: durable_intent.native_skill_selection_vector.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AcceptsLaunchRegistration, CreatesHerdrLaunchPane, ObservesNativeLaunchBinding,
        ObservesNativeTargetReceipt, ResolvesClaudeNativeSkills, StartsNativeHerdrHarness,
        SubmitsFirstPromptOnce, TitlesNativeFlow,
    };
    use crate::composition::{LaunchReceipt, NamesRemoteControl};
    use crate::fixture_executable::{FixtureExecutable, InstallsScript};
    use crate::herdr::HerdrCli;
    use signal_flow::{
        ComposedLaunch, Effort, FirstPromptPayload, FlowAspect, HarnessKind, LaunchProfile,
        ModelName, NativeLaunchBinding, NativeTargetReceipt, NativeTranscriptBoundary, PowerLevel,
        PromptDeliveryIntent, PromptDeliveryResult, RegistrationAcknowledgement,
        TargetReceiptRequest,
    };
    use std::fs;

    const PROMPT_HASH: &str = "0cb26cfe0a554e4780aa5af20cafbe3ae3259f823438576026a4ffff58371a67";

    fn launch(harness_kind: HarnessKind) -> ComposedLaunch {
        ComposedLaunch {
            launch_profile: LaunchProfile {
                launch_request_id: "launch-42".into(),
                launch_source_vector: vec![],
                skill_name_vector: vec![],
                flow_aspect: FlowAspect::Field,
                power_level: PowerLevel::Medium,
                harness_kind,
                model_name: ModelName::from("model-current"),
                effort: Effort::from("high"),
                flow_id_option: None,
                remembered_flow_vector: vec![],
                herdr_session_name: "flowlaunch42".into(),
                system_prompt_bundle_file: "/tmp/flow-system-prompt.md".into(),
                instruction_prompt: "do the work".into(),
            },
            first_prompt_payload: FirstPromptPayload {
                first_prompt_body: "composed body".into(),
                prompt_sha256: PROMPT_HASH.into(),
                first_prompt_text: format!("composed body{}", LaunchReceipt::footer()),
            },
            target_receipt_request: TargetReceiptRequest {
                launch_request_id: "launch-42".into(),
                prompt_sha256: PROMPT_HASH.into(),
            },
        }
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
  *"pane run"*) ;;
  *"pane wait-output"*) marker=$(printf '%s\n' "$*" | sed 's/.*--match \([^ ]*\).*/\1/'); printf '{{"result":{{"pane_id":"w7:p1","matched_line":"%s"}}}}\n' "$marker" ;;
  *"agent start"*) printf '%s\n' '{{"result":{{"agent":{{"name":"{agent_name}"}}}}}}' ;;
  *"agent get"*) reported_harness=$(cat '{reported_harness}'); title=$(cat '{title_file}' 2>/dev/null); printf '{{"result":{{"agent":{{"name":"{agent_name}","agent":"%s","workspace_id":"w7","pane_id":"w7:p1","terminal_id":"term-native","interactive_ready":true,"terminal_title_stripped":"%s","agent_session":{{"source":"herdr:%s","agent":"%s","kind":"id","value":"{native_session}"}}}}}}}}\n' "$reported_harness" "$title" "$reported_harness" "$reported_harness" ;;
  *"agent prompt"*" /rename "*) title=$(printf '%s' "$*" | sed 's/.* \/rename //'); [ -f '{title_override}' ] && title=$(cat '{title_override}'); printf '%s' "$title" > '{title_file}'; printf '{{"type":"custom-title","customTitle":"%s","sessionId":"{native_session}"}}\n' "$title" >> '{claude_transcript}'; printf '%s\n' '{{"result":{{"accepted":true}}}}' ;;
  *"agent prompt"*) printf '%s\n' '{{"result":{{"accepted":true}}}}' ;;
  *"pane rename w7:p1 "*) printf '%s' "$*" | sed 's/.*pane rename w7:p1 //' > '{label_file}'; printf '%s\n' '{{"result":{{}}}}' ;;
  *"pane get w7:p1"*) label=$(cat '{label_file}' 2>/dev/null); printf '{{"result":{{"pane":{{"pane_id":"w7:p1","terminal_id":"term-native","label":"%s"}}}}}}\n' "$label" ;;
  *) exit 8 ;;
esac
"##,
            calls = calls.display(),
            reported_harness = reported_harness.display(),
            title_file = root.path().join("native-title").display(),
            title_override = root.path().join("native-title-override").display(),
            label_file = root.path().join("pane-label").display(),
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
        let flows = root.path().join("flows");
        fs::create_dir(&flows).expect("fixture flows root");
        let transcript_root = root.path().join("native-transcripts");
        fs::create_dir_all(transcript_root.join("codex")).expect("codex transcript root");
        fs::create_dir_all(transcript_root.join("claude")).expect("claude transcript root");
        let flow_id = transcript_root.join("flow-id");
        let flow_id_calls = root.path().join("flow-id-calls");
        let uuid_version = if harness == "claude" {
            "uuid-version=uuid-v4\n"
        } else {
            ""
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
        let skills = match launch.launch_profile.harness_kind {
            HarnessKind::Claude => adapter
                .resolve_claude_native_skills(launch, &binding)
                .expect("resolved Claude skills"),
            HarnessKind::Codex => vec![],
        };
        adapter
            .accept_registration(launch, &binding, &acknowledgement, skills)
            .expect("registered prompt intent")
    }

    #[test]
    fn codex_launch_selects_one_exact_executable_and_remote_socket() {
        let launch = launch(HarnessKind::Codex);
        let agent_name = HerdrCli::launch_agent_name(&launch);
        let (root, adapter) = fixture_herdr(
            "codex",
            "12345678-1234-4abc-8def-123456789abc",
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        adapter
            .start_native_harness(&launch, &pane)
            .expect("started exact Codex client");
        let calls = fs::read_to_string(root.path().join("calls")).expect("launch calls");
        let start = calls
            .lines()
            .find(|line| line.contains("agent start"))
            .expect("agent start call");
        assert!(start.contains("--executable /fixture/codex"));
        assert!(start.contains("-- --remote unix:///tmp/stable-codex.sock --model model-current"));
        assert!(start.contains("-c model_reasoning_effort=high"));
        assert!(!start.contains("--remote-control"));
        assert_eq!(start.matches("--remote").count(), 1);
        assert!(!calls.contains("pane run"));
        assert!(!start.contains(HerdrCli::CLAUDE_SKIP_PERMISSIONS_FLAG));
        // The harness starts with no positional prompt; the one first prompt
        // travels later in a single app-server turn/start.
        assert!(start.ends_with("-c model_reasoning_effort=high"));
        assert!(!start.contains('$'));
        assert!(!calls.contains("agent prompt"));
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
            HerdrCli::claude_environment_preparation("test-marker"),
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

    #[test]
    fn stages_pane_native_claim_registration_and_one_ambiguous_prompt_write() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = HerdrCli::launch_agent_name(&launch);
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        adapter
            .start_native_harness(&launch, &pane)
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
            "-- --dangerously-skip-permissions --settings {{\"permissions\":{{\"defaultMode\":\"bypassPermissions\"}}}} --remote-control {remote_control_name} --system-prompt-file /tmp/flow-system-prompt.md --model model-current --effort high"
        )));
        // The flag settings name the launch's own mode, which suppresses the
        // auto-mode default offer; no settings file is written.
        assert_eq!(start_call.matches(" --settings ").count(), 1);
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
        assert!(calls_after_start.contains("unset CLAUDE_CODE_CHILD_SESSION"));
        let create_call = calls_after_start
            .lines()
            .find(|line| line.contains("workspace create"))
            .expect("workspace create call");
        assert!(create_call.contains(&format!("--cwd {}", root.path().display())));
        assert!(!create_call.contains("/home/li/primary"));
        let binding = adapter
            .observe_native_binding(&launch, &pane)
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
            .accept_registration(&launch, &binding, &acknowledgement, vec![])
            .expect("registered intent");
        assert!(matches!(
            &intent.native_transcript_boundary,
            NativeTranscriptBoundary::Absent(_)
        ));
        assert_eq!(
            adapter
                .submit_first_prompt_once(&launch, &intent)
                .expect("single terminal write"),
            PromptDeliveryResult::Ambiguous(intent)
        );
        let all_calls = fs::read_to_string(root.path().join("calls")).expect("all launch calls");
        assert_eq!(
            all_calls
                .lines()
                .filter(|line| line.contains("agent prompt"))
                .count(),
            1
        );
    }

    #[test]
    fn malformed_full_prompt_is_rejected_before_herdr_prompt_write() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = HerdrCli::launch_agent_name(&launch);
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
        malformed.first_prompt_payload.first_prompt_text.push('x');

        assert_eq!(
            malformed.first_prompt_payload.prompt_sha256,
            unchanged_body_hash
        );
        assert!(
            adapter
                .submit_first_prompt_once(&malformed, &intent)
                .unwrap_err()
                .contains("not canonical")
        );
        let calls = fs::read_to_string(root.path().join("calls")).expect("launch calls");
        assert_eq!(
            calls
                .lines()
                .filter(|line| line.contains("agent prompt"))
                .count(),
            0
        );
    }

    #[test]
    fn missing_official_identity_and_mismatched_claim_fail_closed() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Codex);
        let agent_name = HerdrCli::launch_agent_name(&launch);
        let (root, adapter) = fixture_herdr(
            "codex",
            native_session,
            "ffffffffffffffffffffffffffffffff",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        assert!(
            adapter
                .observe_native_binding(&launch, &pane)
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
                .observe_native_binding(&launch, &missing_pane)
                .unwrap_err()
                .contains("empty native identity")
        );
    }

    #[test]
    fn unsupported_workspace_roots_are_rejected_before_herdr_creation() {
        let launch = launch(HarnessKind::Codex);
        let agent_name = HerdrCli::launch_agent_name(&launch);
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
    fn old_exact_receipt_before_cursor_is_rejected_and_new_receipt_is_observed() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Codex);
        let agent_name = HerdrCli::launch_agent_name(&launch);
        let (root, adapter) = fixture_herdr(
            "codex",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        let marker = LaunchReceipt::MARKER;
        let transcript = root
            .path()
            .join("native-transcripts/codex")
            .join(format!("rollout-{native_session}.jsonl"));
        let stale_receipt = serde_json::json!({"type":"event_msg","payload":{
            "thread_id":native_session,"turn_id":"turn-stale",
            "item":{"type":"AgentMessage","content":[{"type":"Text","text":marker}]}}});
        fs::write(&transcript, format!("{}\n", stale_receipt)).expect("stale transcript");
        let intent = registered_intent(&adapter, &launch, pane, native_session);
        assert!(matches!(
            &intent.native_transcript_boundary,
            NativeTranscriptBoundary::Existing(_)
        ));
        assert_eq!(
            adapter
                .observe_native_target_receipt(&intent)
                .expect("stale receipt is ignored"),
            PromptDeliveryResult::Ambiguous(intent.clone())
        );
        let receipt = serde_json::json!({"type":"event_msg","payload":{
            "thread_id":native_session,"turn_id":"turn-new",
            "item":{"type":"AgentMessage","content":[{"type":"Text","text":marker}]}}});
        let context = serde_json::json!({"type":"turn_context","payload":{
            "turn_id":"turn-new","model":"model-current","effort":"high"}});
        let input = serde_json::json!({"type":"event_msg","payload":{
            "thread_id":native_session,"turn_id":"turn-new",
            "item":{"type":"UserMessage","content":[{"type":"text","text":launch.first_prompt_payload.first_prompt_text}]}}});
        use std::io::Write;
        let mut append = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .expect("append transcript");
        writeln!(append, "{context}").expect("append turn context");
        writeln!(append, "{input}").expect("append native input");
        writeln!(append, "{receipt}").expect("append fresh receipt");
        let result = adapter
            .observe_native_target_receipt(&intent)
            .expect("observed transcript receipt");
        let PromptDeliveryResult::Observed(NativeTargetReceipt {
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
    fn codex_receipt_rejects_wrong_first_text_with_matching_skills_and_marker() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Codex);
        let agent_name = HerdrCli::launch_agent_name(&launch);
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
        let marker = LaunchReceipt::MARKER;
        let rows = [
            serde_json::json!({"type":"turn_context","payload":{
                "turn_id":"turn-wrong","model":"model-current","effort":"high"}}),
            serde_json::json!({"type":"event_msg","payload":{
                "thread_id":native_session,"turn_id":"turn-wrong",
                "item":{"type":"UserMessage","content":[{"type":"text","text":"wrong body with no authenticated footer"}]}}}),
            serde_json::json!({"type":"event_msg","payload":{
                "thread_id":native_session,"turn_id":"turn-wrong",
                "item":{"type":"AgentMessage","content":[{"type":"Text","text":marker}]}}}),
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
                .contains("first-turn text differs")
        );
    }

    #[test]
    fn absent_or_duplicate_native_receipt_is_never_retried_or_invented() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = HerdrCli::launch_agent_name(&launch);
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
        let marker = LaunchReceipt::MARKER;
        let untrusted_row = serde_json::json!({"type":"assistant","sessionId":native_session,
            "uuid":"untrusted-turn","message":{"content":[{"type":"text","text":marker}]}});
        fs::write(&untrusted_transcript, format!("{}\n", untrusted_row))
            .expect("untrusted transcript");
        assert_eq!(
            adapter
                .observe_native_target_receipt(&intent)
                .expect("caller-selected transcript is ignored"),
            PromptDeliveryResult::Ambiguous(intent.clone())
        );
        let transcript = root
            .path()
            .join("native-transcripts/claude")
            .join(format!("{native_session}.jsonl"));
        let first_input = serde_json::json!({"type":"user","sessionId":native_session,
            "message":{"role":"user","content":launch.first_prompt_payload.first_prompt_text}});
        fs::write(&transcript, format!("{first_input}\n")).expect("empty receipt transcript");
        let PromptDeliveryResult::Ambiguous(adopted_intent) = adapter
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
            "content":[{"type":"text","text":marker}]}});
        use std::io::Write;
        let mut append = fs::OpenOptions::new()
            .append(true)
            .open(&transcript)
            .expect("append duplicate receipts");
        writeln!(append, "{row}").expect("first receipt");
        writeln!(append, "{row}").expect("second receipt");
        assert!(
            adapter
                .observe_native_target_receipt(&adopted_intent)
                .unwrap_err()
                .contains("duplicate")
        );
    }

    #[test]
    fn registered_harness_change_with_same_native_id_is_rejected() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = HerdrCli::launch_agent_name(&launch);
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
        let agent_name = HerdrCli::launch_agent_name(&launch);
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

    #[test]
    fn claude_stacked_command_prompt_reaches_its_receipt() {
        use sha2::{Digest, Sha256};
        use std::io::Write;
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let names = [
            "spirit",
            "psyche",
            "main-flow",
            "behavior",
            "herdr",
            "messaging",
            "datom",
        ];
        let mut launch = launch(HarnessKind::Claude);
        launch.launch_profile.skill_name_vector = names.map(String::from).to_vec();
        let body = "/spirit /psyche /main-flow /behavior /herdr # Flow launch\n\nThen load through the Skill tool, in this order: messaging, datom\n\ndo the work";
        let body_hash = format!("{:x}", Sha256::digest(body.as_bytes()));
        launch.first_prompt_payload.first_prompt_body = body.into();
        launch.first_prompt_payload.prompt_sha256 = body_hash.clone();
        launch.first_prompt_payload.first_prompt_text =
            format!("{body}{}", LaunchReceipt::footer());
        launch.target_receipt_request.prompt_sha256 = body_hash;
        let agent_name = HerdrCli::launch_agent_name(&launch);
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let skills = root.path().join("native-transcripts/claude-skills");
        for name in names {
            fs::create_dir_all(skills.join(name)).expect("skill directory");
            fs::write(
                skills.join(name).join("SKILL.md"),
                format!("---\nname: {name}\n---\n\n{name} body\n"),
            )
            .expect("skill source");
        }
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        adapter
            .start_native_harness(&launch, &pane)
            .expect("started without prompt");
        let intent = registered_intent(&adapter, &launch, pane, native_session);
        adapter
            .submit_first_prompt_once(&launch, &intent)
            .expect("single terminal write");
        let calls = fs::read_to_string(root.path().join("calls")).expect("launch calls");
        let prompt_bearing = calls
            .lines()
            .filter(|line| line.contains("agent prompt") || line.contains("# Flow launch"))
            .collect::<Vec<_>>();
        assert_eq!(prompt_bearing.len(), 1, "{calls}");
        assert!(prompt_bearing[0].contains("agent prompt"));

        let base = |name: &str| {
            format!(
                "Base directory for this skill: {}\n\n{name} body\n",
                skills.join(name).canonicalize().unwrap().display()
            )
        };
        // What Claude Code 2.1.280 records for a stacked head: one command
        // record per loaded command, each carrying the text after the whole
        // stack, the first also carrying the original input; each followed
        // by its expansion.
        let argument = format!(
            "{}{}",
            body.strip_prefix("/spirit /psyche /main-flow /behavior /herdr ")
                .unwrap(),
            LaunchReceipt::footer()
        );
        let original = format!("{body}{}", LaunchReceipt::footer());
        let command = |name: &str| {
            let mut row = serde_json::json!({"type":"user","sessionId":native_session,"message":{"role":"user",
                "content":format!("<command-message>{name}</command-message>\n<command-name>/{name}</command-name>\n<command-args>{argument}</command-args>")}});
            if name == "spirit" {
                row["stackedOriginalInput"] = serde_json::json!(original);
            }
            row
        };
        let expansion = |name: &str| {
            serde_json::json!({"type":"user","sessionId":native_session,"isMeta":true,"turnCompanion":true,
                "message":{"role":"user","content":[{"type":"text","text":format!("{}\n\nARGUMENTS: {argument}", base(name))}]}})
        };
        let tool = |name: &str, id: &str| {
            [
                serde_json::json!({"type":"assistant","sessionId":native_session,"uuid":format!("turn-{id}"),
                    "message":{"content":[{"type":"tool_use","id":id,"name":"Skill","input":{"skill":name}}]}}),
                serde_json::json!({"type":"user","sessionId":native_session,
                    "toolUseResult":{"success":true,"commandName":name},
                    "message":{"role":"user","content":[{"type":"tool_result","tool_use_id":id}]}}),
                serde_json::json!({"type":"user","sessionId":native_session,"isMeta":true,"turnCompanion":true,
                    "sourceToolUseID":id,
                    "message":{"role":"user","content":[{"type":"text","text":base(name)}]}}),
            ]
        };
        let receipt = serde_json::json!({"type":"assistant","sessionId":native_session,"uuid":"turn-claude",
            "effort":"high","message":{"model":"model-current",
            "content":[{"type":"text","text":LaunchReceipt::MARKER}]}});
        let mut rows = Vec::new();
        for name in &names[..5] {
            rows.push(command(name));
            rows.push(expansion(name));
        }
        rows.extend(tool("messaging", "tool-6"));
        rows.extend(tool("datom", "tool-7"));
        rows.push(receipt.clone());
        let command_records = rows
            .iter()
            .filter(|row| {
                row.pointer("/message/content")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|text| text.starts_with("<command-message>"))
            })
            .count();
        assert!(command_records <= 5);
        let transcript = root
            .path()
            .join("native-transcripts/claude")
            .join(format!("{native_session}.jsonl"));
        let write = |rows: &[serde_json::Value]| {
            let mut output = fs::File::create(&transcript).expect("transcript");
            for row in rows {
                writeln!(output, "{row}").expect("row");
            }
        };
        write(&rows);
        let PromptDeliveryResult::Observed(receipt_observed) = adapter
            .observe_native_target_receipt(&intent)
            .expect("observed receipt")
        else {
            panic!("receipt remained ambiguous");
        };
        assert_eq!(receipt_observed.native_turn_id, "turn-claude");

        // A wrong leading command.
        let mut wrong = rows.clone();
        wrong[0] = command("main-flow");
        write(&wrong);
        assert!(
            adapter
                .observe_native_target_receipt(&intent)
                .unwrap_err()
                .contains("first-turn text differs")
        );

        // A sixth command record: the stack holds five.
        let mut sixth = rows[..10].to_vec();
        sixth.push(command("messaging"));
        sixth.push(expansion("messaging"));
        sixth.extend(tool("datom", "tool-7"));
        sixth.push(receipt.clone());
        write(&sixth);
        assert!(
            adapter
                .observe_native_target_receipt(&intent)
                .unwrap_err()
                .contains("stacked command differs")
        );

        // A stacked command out of order.
        let mut swapped = rows.clone();
        swapped.swap(2, 4);
        swapped.swap(3, 5);
        write(&swapped);
        assert!(
            adapter
                .observe_native_target_receipt(&intent)
                .unwrap_err()
                .contains("stacked command differs")
        );

        // Only four commands loaded, then the Skill tool.
        let mut short = rows[..8].to_vec();
        short.extend(tool("herdr", "tool-5"));
        short.extend(tool("messaging", "tool-6"));
        short.extend(tool("datom", "tool-7"));
        short.push(receipt);
        write(&short);
        assert!(
            adapter
                .observe_native_target_receipt(&intent)
                .unwrap_err()
                .contains("Skill invocation order differs")
        );
    }

    #[test]
    fn claude_title_is_set_after_the_claim_and_read_back() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let mut launch = launch(HarnessKind::Claude);
        launch.launch_profile.flow_aspect = FlowAspect::Psyche;
        launch.launch_profile.model_name = "claude-fable-5-1".into();
        let agent_name = HerdrCli::launch_agent_name(&launch);
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).expect("created pane");
        adapter
            .start_native_harness(&launch, &pane)
            .expect("started without prompt");
        let binding = adapter
            .observe_native_binding(&launch, &pane)
            .expect("claimed binding");
        let title = adapter
            .title_native_flow(&launch, &binding)
            .expect("title set and read back");
        let expected = "PsycheV2.{ Fable 123456 }";
        assert_eq!(title.as_str(), expected);
        // Readback: the terminal title Herdr reports, the session's
        // transcript title record, and the Herdr pane label all equal it.
        assert_eq!(
            fs::read_to_string(root.path().join("native-title")).unwrap(),
            expected
        );
        assert_eq!(
            adapter
                .claude_transcript_title(&pane, native_session)
                .unwrap()
                .as_deref(),
            Some(expected)
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
        launch.launch_profile.flow_aspect = FlowAspect::Psyche;
        launch.launch_profile.model_name = "claude-fable-5-1".into();
        let agent_name = HerdrCli::launch_agent_name(&launch);
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
        adapter.start_native_harness(&launch, &pane).unwrap();
        let binding = adapter.observe_native_binding(&launch, &pane).unwrap();
        assert!(
            adapter
                .title_native_flow(&launch, &binding)
                .unwrap_err()
                .contains("readback differs")
        );
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        assert!(!calls.contains("pane rename"));
    }

    #[test]
    fn unmapped_model_is_refused_before_any_rename() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let launch = launch(HarnessKind::Claude);
        let agent_name = HerdrCli::launch_agent_name(&launch);
        let (root, adapter) = fixture_herdr(
            "claude",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let pane = adapter.create_launch_pane(&launch).unwrap();
        adapter.start_native_harness(&launch, &pane).unwrap();
        let binding = adapter.observe_native_binding(&launch, &pane).unwrap();
        assert!(
            adapter
                .title_native_flow(&launch, &binding)
                .unwrap_err()
                .contains("unmapped exact native model identifier: model-current")
        );
        let calls = fs::read_to_string(root.path().join("calls")).unwrap();
        assert!(!calls.contains("/rename"));
        assert!(!calls.contains("pane rename"));
    }

    #[test]
    fn codex_title_is_set_through_the_app_server_and_read_back() {
        let native_session = "12345678-1234-4abc-8def-123456789abc";
        let mut launch = launch(HarnessKind::Codex);
        launch.launch_profile.model_name = "gpt-6-astra".into();
        let agent_name = HerdrCli::launch_agent_name(&launch);
        let (root, mut adapter) = fixture_herdr(
            "codex",
            native_session,
            "1234567812344abc8def123456789abc",
            &agent_name,
        );
        let expected = "FieldV2.{ Astra 123456 }";
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
        let binding = adapter.observe_native_binding(&launch, &pane).unwrap();
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
