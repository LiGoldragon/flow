//! Staged native launch through Herdr's documented CLI surface.
//!
//! Launching, claiming an identity, registering it, submitting the first
//! prompt, and observing its target-side receipt are separate operations.
//! The adapter never chooses registration policy and never retries a prompt
//! whose terminal write may already have succeeded.

use super::{DecodesFlowClaim, FlowClaim, HerdrCli};
use sha2::{Digest, Sha256};
use signal_flow::{
    ComposedLaunch, HarnessKind, HerdrPaneBinding, NativeLaunchBinding, NativeTargetReceipt,
    PromptDeliveryIntent, PromptDeliveryResult, RegistrationAcknowledgement,
};
use std::{
    fs,
    io::{BufRead, BufReader},
    path::Path,
    process::Command,
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

/// Submits the first prompt once. A successful Herdr write remains ambiguous
/// until the native transcript independently contains the requested receipt.
pub trait SubmitsFirstPromptOnce {
    fn submit_first_prompt_once(
        &self,
        launch: &ComposedLaunch,
        durable_intent: &PromptDeliveryIntent,
    ) -> Result<PromptDeliveryResult, String>;
}

/// Promotes an ambiguous delivery only from an exact assistant record in the
/// native transcript for the bound session and turn.
pub trait ObservesNativeTargetReceipt {
    fn observe_native_target_receipt(
        &self,
        durable_intent: &PromptDeliveryIntent,
    ) -> Result<PromptDeliveryResult, String>;
}

impl HerdrCli {
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
        Self::binding_matches_launch(launch, &intent.herdr_pane_binding)?;
        if intent.launch_request_id != launch.launch_profile.launch_request_id
            || intent.prompt_sha256 != launch.first_prompt_payload.prompt_sha256
        {
            return Err("durable prompt intent does not belong to this composed launch".into());
        }
        Ok(())
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

    fn native_harness_for_intent(
        &self,
        intent: &PromptDeliveryIntent,
    ) -> Result<HarnessKind, String> {
        let pane = &intent.herdr_pane_binding;
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
        let harness = match agent.get("agent").and_then(serde_json::Value::as_str) {
            Some("codex") => HarnessKind::Codex,
            Some("claude") => HarnessKind::Claude,
            _ => return Err("receipt target has an unsupported native harness".into()),
        };
        let expected_harness = Self::expected_harness(&harness);
        if session.get("source").and_then(serde_json::Value::as_str)
            != Some(format!("herdr:{expected_harness}").as_str())
            || session.get("agent").and_then(serde_json::Value::as_str) != Some(expected_harness)
            || session.get("kind").and_then(serde_json::Value::as_str) != Some("id")
            || session.get("value").and_then(serde_json::Value::as_str)
                != Some(intent.native_session_id.as_str())
        {
            return Err("receipt target native identity is no longer exact".into());
        }
        Ok(harness)
    }

    fn collect_native_transcripts(
        directory: &Path,
        native_session_id: &str,
        harness: &HarnessKind,
        depth: usize,
        found: &mut Vec<std::path::PathBuf>,
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

    fn resolve_native_transcript(
        &self,
        intent: &PromptDeliveryIntent,
    ) -> Result<std::path::PathBuf, String> {
        let harness = self.native_harness_for_intent(intent)?;
        let root = match harness {
            HarnessKind::Codex => &self.codex_transcript_root,
            HarnessKind::Claude => &self.claude_transcript_root,
        };
        let canonical_root = root.canonicalize().map_err(|error| {
            format!("configured native transcript root is unavailable: {error}")
        })?;
        let mut found = Vec::new();
        Self::collect_native_transcripts(
            &canonical_root,
            &intent.native_session_id,
            &harness,
            0,
            &mut found,
        )?;
        if found.len() != 1 {
            return Err("native transcript identity did not resolve to exactly one file".into());
        }
        let resolved = found
            .pop()
            .expect("one native transcript")
            .canonicalize()
            .map_err(|error| format!("native transcript could not be resolved: {error}"))?;
        if !resolved.starts_with(&canonical_root) {
            return Err("native transcript resolved outside its configured root".into());
        }
        Ok(resolved)
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
            "--".into(),
            "--model".into(),
            launch.launch_profile.model_name.clone(),
        ];
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

impl AcceptsLaunchRegistration for HerdrCli {
    fn accept_registration(
        &self,
        launch: &ComposedLaunch,
        binding: &NativeLaunchBinding,
        acknowledgement: &RegistrationAcknowledgement,
    ) -> Result<PromptDeliveryIntent, String> {
        Self::binding_matches_launch(launch, &binding.herdr_pane_binding)?;
        if acknowledgement.launch_request_id != binding.launch_request_id
            || acknowledgement.flow_id != binding.flow_id
            || acknowledgement.native_session_id != binding.native_session_id
            || acknowledgement.herdr_pane_binding != binding.herdr_pane_binding
        {
            return Err("registration acknowledgement does not match native binding".into());
        }
        Ok(PromptDeliveryIntent {
            launch_request_id: binding.launch_request_id.clone(),
            prompt_sha256: launch.first_prompt_payload.prompt_sha256.clone(),
            flow_id: binding.flow_id.clone(),
            native_session_id: binding.native_session_id.clone(),
            herdr_pane_binding: binding.herdr_pane_binding.clone(),
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
        let expected = format!(
            "FLOW_LAUNCH_RECEIPT_V1 launch_request_id={} prompt_body_sha256={}",
            durable_intent.launch_request_id, durable_intent.prompt_sha256
        );
        let transcript = self.resolve_native_transcript(durable_intent)?;
        let input = fs::File::open(transcript)
            .map_err(|error| format!("native transcript is unreadable: {error}"))?;
        let mut observed_turn = None;
        for line in BufReader::new(input).lines() {
            let line = line.map_err(|error| format!("native transcript read failed: {error}"))?;
            let Ok(row) = serde_json::from_str::<serde_json::Value>(&line) else {
                continue;
            };
            match Self::assistant_receipt(&row, &durable_intent.native_session_id, &expected) {
                Some(_) if observed_turn.is_some() => {
                    return Err("native transcript contains duplicate target receipts".into());
                }
                Some(turn) => observed_turn = Some(turn),
                None => {}
            }
        }
        let Some(native_turn_id) = observed_turn else {
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
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AcceptsLaunchRegistration, CreatesHerdrLaunchPane, ObservesNativeLaunchBinding,
        ObservesNativeTargetReceipt, StartsNativeHerdrHarness, SubmitsFirstPromptOnce,
    };
    use crate::herdr::HerdrCli;
    use signal_flow::{
        ComposedLaunch, Effort, FirstPromptPayload, FlowAspect, HarnessKind, LaunchProfile,
        ModelName, NativeTargetReceipt, PowerLevel, PromptDeliveryResult,
        RegistrationAcknowledgement, TargetReceiptRequest,
    };
    use std::{fs, os::unix::fs::PermissionsExt};

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
                instruction_prompt: "do the work".into(),
            },
            first_prompt_payload: FirstPromptPayload {
                first_prompt_body: "composed body".into(),
                prompt_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .into(),
                first_prompt_text: "composed body\n\nreceipt footer".into(),
            },
            target_receipt_request: TargetReceiptRequest {
                launch_request_id: "launch-42".into(),
                prompt_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .into(),
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
        let script = format!(
            r##"#!/bin/sh
printf '%s\n' "$*" >> '{calls}'
case "$*" in
  *"workspace create"*) printf '%s\n' '{{"result":{{"workspace":{{"workspace_id":"w7"}},"root_pane":{{"pane_id":"w7:p1","terminal_id":"term-native"}}}}}}' ;;
  *"agent start"*) printf '%s\n' '{{"result":{{"agent":{{"name":"{agent_name}"}}}}}}' ;;
  *"agent get"*) printf '%s\n' '{{"result":{{"agent":{{"name":"{agent_name}","agent":"{harness}","workspace_id":"w7","pane_id":"w7:p1","terminal_id":"term-native","interactive_ready":true,"agent_session":{{"source":"herdr:{harness}","agent":"{harness}","kind":"id","value":"{native_session}"}}}}}}}}' ;;
  *"agent prompt"*) printf '%s\n' '{{"result":{{"accepted":true}}}}' ;;
  *) exit 8 ;;
esac
"##,
            calls = calls.display()
        );
        fs::write(&executable, script).expect("fixture executable");
        fs::File::open(&executable)
            .expect("open fixture executable")
            .sync_all()
            .expect("sync fixture executable");
        let mut permissions = fs::metadata(&executable)
            .expect("fixture metadata")
            .permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&executable, permissions).expect("fixture permissions");
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
        fs::write(&flow_id, flow_id_script).expect("flow-id fixture executable");
        let mut flow_id_permissions = fs::metadata(&flow_id)
            .expect("flow-id fixture metadata")
            .permissions();
        flow_id_permissions.set_mode(0o700);
        fs::set_permissions(&flow_id, flow_id_permissions).expect("flow-id fixture permissions");
        let adapter = HerdrCli::at(executable, flows);
        (root, adapter)
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
        assert!(start_call.contains("--model model-current --effort high"));
        assert!(!start_call.contains("composed body"));
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
            .accept_registration(&launch, &binding, &acknowledgement)
            .expect("registered intent");
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
    fn only_exact_assistant_native_receipt_promotes_ambiguous_delivery() {
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
        let intent = signal_flow::PromptDeliveryIntent {
            launch_request_id: "launch-42".into(),
            prompt_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .into(),
            flow_id: "123456".into(),
            native_session_id: native_session.into(),
            herdr_pane_binding: pane,
        };
        let marker = "FLOW_LAUNCH_RECEIPT_V1 launch_request_id=launch-42 prompt_body_sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let transcript = root
            .path()
            .join("native-transcripts/codex")
            .join(format!("rollout-{native_session}.jsonl"));
        let wrong_role = serde_json::json!({"type":"event_msg","payload":{
            "thread_id":native_session,"turn_id":"turn-user",
            "item":{"type":"UserMessage","content":[{"text":marker}]}}});
        let receipt = serde_json::json!({"type":"event_msg","payload":{
            "thread_id":native_session,"turn_id":"turn-native",
            "item":{"type":"AgentMessage","content":[{"type":"Text","text":marker}]}}});
        fs::write(&transcript, format!("{}\n{}\n", wrong_role, receipt))
            .expect("native transcript");
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
        assert_eq!(native_turn_id, "turn-native");
        assert_eq!(receipt_sha256.len(), 64);
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
        let intent = signal_flow::PromptDeliveryIntent {
            launch_request_id: "launch-42".into(),
            prompt_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .into(),
            flow_id: "123456".into(),
            native_session_id: native_session.into(),
            herdr_pane_binding: pane,
        };
        let untrusted_transcript = root.path().join("caller-selected.jsonl");
        let marker = "FLOW_LAUNCH_RECEIPT_V1 launch_request_id=launch-42 prompt_body_sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let untrusted_row = serde_json::json!({"type":"assistant","sessionId":native_session,
            "uuid":"untrusted-turn","message":{"content":[{"type":"text","text":marker}]}});
        fs::write(&untrusted_transcript, format!("{}\n", untrusted_row))
            .expect("untrusted transcript");
        assert!(
            adapter
                .observe_native_target_receipt(&intent)
                .unwrap_err()
                .contains("exactly one file")
        );
        let transcript = root
            .path()
            .join("native-transcripts/claude")
            .join(format!("{native_session}.jsonl"));
        fs::write(&transcript, "{\"type\":\"user\"}\n").expect("empty receipt transcript");
        assert_eq!(
            adapter
                .observe_native_target_receipt(&intent)
                .expect("absence is ambiguous"),
            PromptDeliveryResult::Ambiguous(intent.clone())
        );
        let row = serde_json::json!({"type":"assistant","sessionId":native_session,
            "uuid":"turn-claude","message":{"content":[{"type":"text","text":marker}]}});
        fs::write(&transcript, format!("{}\n{}\n", row, row)).expect("duplicate receipts");
        assert!(
            adapter
                .observe_native_target_receipt(&intent)
                .unwrap_err()
                .contains("duplicate")
        );
    }
}
