//! Source-backed readiness evidence used by Start and refresh reconciliation.

use std::{
    fs::{self, File},
    io::{BufRead, BufReader},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    process::Command,
};

use sha2::{Digest, Sha256};
use signal_flow::{
    HandoverSelection, HarnessKind, NativeLaunchBinding, NativeTargetReceipt, ProcessIdentity,
    RefreshPolicy, RefreshRejection, TranscriptHandoverReference, TranscriptRole,
};

use super::HerdrCli;
use crate::refresh::ReadsProcessIdentity;

/// Re-observes the exact registered Herdr/native tuple and its foreground
/// harness process before a receipt can make a Flow delivery-eligible.
pub trait ObservesReadyProcessIdentity {
    fn observe_ready_process_identity(
        &self,
        binding: &NativeLaunchBinding,
        receipt: &NativeTargetReceipt,
        process_evidence: &dyn ReadsProcessIdentity,
    ) -> Result<ProcessIdentity, String>;
}

/// Resolves and checks a handover only from the configured native transcript.
pub trait ValidatesTranscriptHandover {
    fn validate_transcript_handover(
        &self,
        binding: &NativeLaunchBinding,
        receipt: &NativeTargetReceipt,
        reference: &TranscriptHandoverReference,
        policy: &RefreshPolicy,
        now_seconds: i64,
    ) -> Result<(), RefreshRejection>;
}

impl HerdrCli {
    fn refresh_json(&self, arguments: &[String]) -> Result<serde_json::Value, String> {
        let output = Command::new(&self.executable)
            .args(arguments)
            .output()
            .map_err(|error| format!("Herdr refresh observation failed: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "Herdr refresh observation was refused: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        serde_json::from_slice(&output.stdout)
            .map_err(|error| format!("Herdr refresh receipt is invalid JSON: {error}"))
    }

    fn expected_refresh_harness(harness_kind: &HarnessKind) -> &'static str {
        match harness_kind {
            HarnessKind::Codex => "codex",
            HarnessKind::Claude => "claude",
        }
    }

    fn collect_refresh_transcripts(
        directory: &Path,
        native_session_id: &str,
        harness_kind: &HarnessKind,
        depth: usize,
        found: &mut Vec<PathBuf>,
    ) -> Result<(), RefreshRejection> {
        if depth > 8 || found.len() > 1 {
            return Ok(());
        }
        for entry in
            fs::read_dir(directory).map_err(|_| RefreshRejection::HandoverReferenceInvalid)?
        {
            let entry = entry.map_err(|_| RefreshRejection::HandoverReferenceInvalid)?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)
                .map_err(|_| RefreshRejection::HandoverReferenceInvalid)?;
            if metadata.file_type().is_symlink() {
                continue;
            }
            if metadata.is_dir() {
                Self::collect_refresh_transcripts(
                    &path,
                    native_session_id,
                    harness_kind,
                    depth + 1,
                    found,
                )?;
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let matches = match harness_kind {
                HarnessKind::Codex => name.ends_with(".jsonl") && name.contains(native_session_id),
                HarnessKind::Claude => name == format!("{native_session_id}.jsonl"),
            };
            if metadata.is_file() && matches {
                found.push(path);
            }
        }
        Ok(())
    }

    fn refresh_transcript(
        &self,
        native_session_id: &str,
        harness_kind: &HarnessKind,
    ) -> Result<File, RefreshRejection> {
        let configured_root = match harness_kind {
            HarnessKind::Codex => &self.codex_transcript_root,
            HarnessKind::Claude => &self.claude_transcript_root,
        };
        let root = configured_root
            .canonicalize()
            .map_err(|_| RefreshRejection::HandoverReferenceInvalid)?;
        let before = fs::metadata(&root).map_err(|_| RefreshRejection::HandoverReferenceInvalid)?;
        if !before.is_dir() {
            return Err(RefreshRejection::HandoverReferenceInvalid);
        }
        let mut found = Vec::new();
        Self::collect_refresh_transcripts(&root, native_session_id, harness_kind, 0, &mut found)?;
        let after = fs::metadata(&root).map_err(|_| RefreshRejection::HandoverReferenceInvalid)?;
        if before.dev() != after.dev() || before.ino() != after.ino() || found.len() != 1 {
            return Err(RefreshRejection::HandoverReferenceInvalid);
        }
        let path = found
            .pop()
            .expect("exactly one transcript")
            .canonicalize()
            .map_err(|_| RefreshRejection::HandoverReferenceInvalid)?;
        if !path.starts_with(&root) {
            return Err(RefreshRejection::HandoverReferenceInvalid);
        }
        File::open(path).map_err(|_| RefreshRejection::HandoverReferenceInvalid)
    }

    fn assistant_text<'a>(
        row: &'a serde_json::Value,
        native_session_id: &str,
        harness_kind: &HarnessKind,
        native_turn_id: &str,
        transcript_item_id: &str,
    ) -> Option<&'a str> {
        let contents = match harness_kind {
            HarnessKind::Codex
                if row.get("type").and_then(serde_json::Value::as_str) == Some("event_msg")
                    && row
                        .pointer("/payload/thread_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(native_session_id)
                    && row
                        .pointer("/payload/turn_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(native_turn_id)
                    && row
                        .pointer("/payload/item/type")
                        .and_then(serde_json::Value::as_str)
                        == Some("AgentMessage")
                    && row
                        .pointer("/payload/item/id")
                        .and_then(serde_json::Value::as_str)
                        == Some(transcript_item_id) =>
            {
                row.pointer("/payload/item/content")
            }
            HarnessKind::Claude
                if row.get("type").and_then(serde_json::Value::as_str) == Some("assistant")
                    && row
                        .get("sessionId")
                        .or_else(|| row.get("session_id"))
                        .and_then(serde_json::Value::as_str)
                        == Some(native_session_id)
                    && row.get("uuid").and_then(serde_json::Value::as_str)
                        == Some(transcript_item_id)
                    && transcript_item_id == native_turn_id
                    && row
                        .pointer("/message/role")
                        .and_then(serde_json::Value::as_str)
                        == Some("assistant") =>
            {
                row.pointer("/message/content")
            }
            _ => None,
        }?;
        let contents = contents.as_array()?;
        if contents.len() != 1 {
            return None;
        }
        let content = &contents[0];
        match harness_kind {
            HarnessKind::Codex
                if content.get("type").and_then(serde_json::Value::as_str) == Some("Text") => {}
            HarnessKind::Claude
                if content.get("type").and_then(serde_json::Value::as_str) == Some("text") => {}
            _ => return None,
        }
        content.get("text").and_then(serde_json::Value::as_str)
    }

    fn receipt_record_matches(
        row: &serde_json::Value,
        receipt: &NativeTargetReceipt,
        expected: &str,
        harness_kind: &HarnessKind,
    ) -> bool {
        let item_id = match harness_kind {
            HarnessKind::Codex => row
                .pointer("/payload/item/id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("receipt-item"),
            HarnessKind::Claude => receipt.native_turn_id.as_str(),
        };
        Self::assistant_text(
            row,
            &receipt.native_session_id,
            harness_kind,
            &receipt.native_turn_id,
            item_id,
        ) == Some(expected)
    }

    fn timestamp_seconds(row: &serde_json::Value) -> Option<i64> {
        let timestamp = row.get("timestamp")?.as_str()?;
        let date = timestamp.get(0..10)?;
        let time = timestamp.get(11..19)?;
        if timestamp.as_bytes().get(10) != Some(&b'T') || !timestamp.ends_with('Z') {
            return None;
        }
        let mut date = date.split('-').map(|part| part.parse::<i64>().ok());
        let year = date.next()??;
        let month = date.next()??;
        let day = date.next()??;
        if date.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
            return None;
        }
        let mut time = time.split(':').map(|part| part.parse::<i64>().ok());
        let hour = time.next()??;
        let minute = time.next()??;
        let second = time.next()??;
        if time.next().is_some() || hour > 23 || minute > 59 || second > 60 {
            return None;
        }
        let adjusted_year = year - i64::from(month <= 2);
        let era = adjusted_year.div_euclid(400);
        let year_of_era = adjusted_year - era * 400;
        let adjusted_month = month + if month > 2 { -3 } else { 9 };
        let day_of_year = (153 * adjusted_month + 2) / 5 + day - 1;
        let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
        let days = era * 146_097 + day_of_era - 719_468;
        days.checked_mul(86_400)?
            .checked_add(hour * 3_600 + minute * 60 + second)
    }

    fn first_text_line(text: &str) -> &str {
        let line = text.split_once('\n').map_or(text, |(line, _)| line);
        line.strip_suffix('\r').unwrap_or(line)
    }
}

impl ObservesReadyProcessIdentity for HerdrCli {
    fn observe_ready_process_identity(
        &self,
        binding: &NativeLaunchBinding,
        receipt: &NativeTargetReceipt,
        process_evidence: &dyn ReadsProcessIdentity,
    ) -> Result<ProcessIdentity, String> {
        if receipt.launch_request_id != binding.launch_request_id
            || receipt.flow_id != binding.flow_id
            || receipt.native_session_id != binding.native_session_id
        {
            return Err("native receipt does not identify the registered binding".into());
        }
        let agent = self
            .refresh_json(&[
                "--session".into(),
                binding.herdr_pane_binding.herdr_session_name.clone(),
                "agent".into(),
                "get".into(),
                binding.herdr_pane_binding.herdr_agent_name.clone(),
            ])?
            .pointer("/result/agent")
            .cloned()
            .ok_or_else(|| "Herdr agent get returned no registered target".to_owned())?;
        let expected_harness = Self::expected_refresh_harness(&binding.harness_kind);
        let session = agent
            .get("agent_session")
            .ok_or_else(|| "registered target has no official native session".to_owned())?;
        if agent.get("name").and_then(serde_json::Value::as_str)
            != Some(binding.herdr_pane_binding.herdr_agent_name.as_str())
            || agent
                .get("workspace_id")
                .and_then(serde_json::Value::as_str)
                != Some(binding.herdr_pane_binding.herdr_workspace_id.as_str())
            || agent.get("pane_id").and_then(serde_json::Value::as_str)
                != Some(binding.herdr_pane_binding.herdr_pane_id.as_str())
            || agent.get("terminal_id").and_then(serde_json::Value::as_str)
                != Some(binding.herdr_pane_binding.herdr_terminal_id.as_str())
            || agent.get("agent").and_then(serde_json::Value::as_str) != Some(expected_harness)
            || agent
                .get("interactive_ready")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
            || session.get("source").and_then(serde_json::Value::as_str)
                != Some(format!("herdr:{expected_harness}").as_str())
            || session.get("agent").and_then(serde_json::Value::as_str) != Some(expected_harness)
            || session.get("kind").and_then(serde_json::Value::as_str) != Some("id")
            || session.get("value").and_then(serde_json::Value::as_str)
                != Some(binding.native_session_id.as_str())
        {
            return Err("registered target is not exactly ready at its native binding".into());
        }

        let process_info = self.refresh_json(&[
            "--session".into(),
            binding.herdr_pane_binding.herdr_session_name.clone(),
            "pane".into(),
            "process-info".into(),
            "--pane".into(),
            binding.herdr_pane_binding.herdr_pane_id.clone(),
        ])?;
        if process_info
            .pointer("/result/process_info/pane_id")
            .and_then(serde_json::Value::as_str)
            != Some(binding.herdr_pane_binding.herdr_pane_id.as_str())
        {
            return Err("Herdr process receipt names another pane".into());
        }
        let foreground = process_info
            .pointer("/result/process_info/foreground_processes")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| "Herdr process receipt has no foreground vector".to_owned())?;
        let [process] = foreground.as_slice() else {
            return Err("registered pane must have exactly one foreground harness process".into());
        };
        let process_id = process
            .get("pid")
            .and_then(serde_json::Value::as_i64)
            .filter(|process_id| *process_id > 0)
            .ok_or_else(|| "foreground harness process has no valid PID".to_owned())?;
        process_evidence
            .process_identity(process_id)
            .map_err(|_| "foreground harness process identity could not be pinned".to_owned())
    }
}

impl ValidatesTranscriptHandover for HerdrCli {
    fn validate_transcript_handover(
        &self,
        binding: &NativeLaunchBinding,
        receipt: &NativeTargetReceipt,
        reference: &TranscriptHandoverReference,
        policy: &RefreshPolicy,
        now_seconds: i64,
    ) -> Result<(), RefreshRejection> {
        if reference.transcript_role != TranscriptRole::Assistant {
            return Err(RefreshRejection::HandoverRoleMismatch);
        }
        if reference.harness_kind != binding.harness_kind
            || reference.native_session_id != binding.native_session_id
            || receipt.native_session_id != binding.native_session_id
            || receipt.flow_id != binding.flow_id
        {
            return Err(RefreshRejection::HandoverReferenceInvalid);
        }
        let expected_receipt = format!(
            "FLOW_LAUNCH_RECEIPT_V1 launch_request_id={} prompt_body_sha256={}",
            receipt.launch_request_id, receipt.prompt_sha256
        );
        if format!("{:x}", Sha256::digest(expected_receipt.as_bytes())) != receipt.receipt_sha256 {
            return Err(RefreshRejection::HandoverBeforeCallerReceipt);
        }
        let file = self.refresh_transcript(&binding.native_session_id, &binding.harness_kind)?;
        let before = file
            .metadata()
            .map_err(|_| RefreshRejection::HandoverReferenceInvalid)?;
        let mut receipt_position = None;
        let mut handover_position = None;
        let mut reader = BufReader::new(file);
        let mut position = 0_u64;
        loop {
            let mut record = Vec::new();
            let read = reader
                .read_until(b'\n', &mut record)
                .map_err(|_| RefreshRejection::HandoverReferenceInvalid)?;
            if read == 0 {
                break;
            }
            if record.last() != Some(&b'\n') {
                return Err(RefreshRejection::HandoverReferenceInvalid);
            }
            record.pop();
            if record.last() == Some(&b'\r') {
                record.pop();
            }
            let row: serde_json::Value = serde_json::from_slice(&record)
                .map_err(|_| RefreshRejection::HandoverReferenceInvalid)?;
            if Self::receipt_record_matches(&row, receipt, &expected_receipt, &binding.harness_kind)
            {
                if receipt_position.replace(position).is_some() {
                    return Err(RefreshRejection::HandoverBeforeCallerReceipt);
                }
            }
            if let Some(text) = Self::assistant_text(
                &row,
                &binding.native_session_id,
                &binding.harness_kind,
                &reference.native_turn_id,
                &reference.transcript_item_id,
            ) {
                if handover_position.is_some() {
                    return Err(RefreshRejection::HandoverReferenceInvalid);
                }
                let digest = format!("{:x}", Sha256::digest(&record));
                if digest != reference.transcript_record_sha256 {
                    return Err(RefreshRejection::HandoverReferenceInvalid);
                }
                let title = Self::first_text_line(text);
                if title != reference.transcript_title
                    || !(title.starts_with("Refresh Payload —") || title.starts_with("Handoff —"))
                {
                    return Err(RefreshRejection::HandoverTitleMismatch);
                }
                let timestamp = Self::timestamp_seconds(&row)
                    .ok_or(RefreshRejection::HandoverReferenceInvalid)?;
                if timestamp != reference.transcript_timestamp_seconds {
                    return Err(RefreshRejection::HandoverReferenceInvalid);
                }
                let age = now_seconds
                    .checked_sub(timestamp)
                    .ok_or(RefreshRejection::StaleHandover)?;
                if age < 0 || age > policy.maximum_handover_age_seconds {
                    return Err(RefreshRejection::StaleHandover);
                }
                if let HandoverSelection::SelectedBytes(selection) = &reference.handover_selection {
                    let offset = usize::try_from(selection.handover_byte_offset)
                        .map_err(|_| RefreshRejection::HandoverReferenceInvalid)?;
                    let length = usize::try_from(selection.handover_byte_length)
                        .map_err(|_| RefreshRejection::HandoverReferenceInvalid)?;
                    let selected = text
                        .as_bytes()
                        .get(offset..offset.saturating_add(length))
                        .ok_or(RefreshRejection::HandoverReferenceInvalid)?;
                    if format!("{:x}", Sha256::digest(selected))
                        != selection.handover_selection_sha256
                    {
                        return Err(RefreshRejection::HandoverReferenceInvalid);
                    }
                }
                handover_position = Some(position);
            }
            position = position
                .checked_add(read as u64)
                .ok_or(RefreshRejection::HandoverReferenceInvalid)?;
        }
        let after = reader
            .into_inner()
            .metadata()
            .map_err(|_| RefreshRejection::HandoverReferenceInvalid)?;
        if before.dev() != after.dev() || before.ino() != after.ino() || before.len() != after.len()
        {
            return Err(RefreshRejection::HandoverReferenceInvalid);
        }
        match (receipt_position, handover_position) {
            (Some(receipt), Some(handover)) if handover > receipt => Ok(()),
            (None, _) | (Some(_), None) => Err(RefreshRejection::HandoverReferenceInvalid),
            _ => Err(RefreshRejection::HandoverBeforeCallerReceipt),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Write};

    use sha2::{Digest, Sha256};
    use signal_flow::{
        HandoverSelection, HarnessKind, HerdrPaneBinding, NativeLaunchBinding, NativeTargetReceipt,
        RefreshPolicy, RefreshRejection, TranscriptHandoverReference, TranscriptRole,
    };

    use super::ValidatesTranscriptHandover;
    use crate::herdr::HerdrCli;

    struct HandoverFixture {
        _directory: tempfile::TempDir,
        herdr: HerdrCli,
        binding: NativeLaunchBinding,
        receipt: NativeTargetReceipt,
        reference: TranscriptHandoverReference,
    }

    trait CreatesHandoverFixture {
        fn canonical(handover_before_receipt: bool) -> Self;
    }

    impl CreatesHandoverFixture for HandoverFixture {
        fn canonical(handover_before_receipt: bool) -> Self {
            let directory = tempfile::tempdir().expect("temporary handover fixture");
            let flows_root = directory.path().join("flows");
            let transcript_root = directory.path().join("native-transcripts/codex");
            fs::create_dir_all(&flows_root).unwrap();
            fs::create_dir_all(&transcript_root).unwrap();
            let herdr = HerdrCli::at(directory.path().join("unused-herdr"), flows_root);
            let session = "01a0d050-6177-7ca1-b413-7fa935691225";
            let binding = NativeLaunchBinding {
                launch_request_id: "launch-1".into(),
                flow_id: "47764b".into(),
                native_session_id: session.into(),
                harness_kind: HarnessKind::Codex,
                herdr_pane_binding: HerdrPaneBinding {
                    launch_request_id: "launch-1".into(),
                    herdr_session_name: "flow".into(),
                    herdr_agent_name: "mind".into(),
                    herdr_workspace_id: "workspace".into(),
                    herdr_pane_id: "w1:p1".into(),
                    herdr_terminal_id: "terminal".into(),
                },
            };
            let marker =
                "FLOW_LAUNCH_RECEIPT_V1 launch_request_id=launch-1 prompt_body_sha256=body-sha";
            let receipt = NativeTargetReceipt {
                launch_request_id: "launch-1".into(),
                prompt_sha256: "body-sha".into(),
                flow_id: "47764b".into(),
                native_session_id: session.into(),
                native_turn_id: "receipt-turn".into(),
                receipt_sha256: format!("{:x}", Sha256::digest(marker.as_bytes())),
                model_name: "gpt-6".into(),
                effort: "high".into(),
                native_skill_selection_vector: Vec::new(),
            };
            let receipt_row = serde_json::json!({
                "timestamp":"2026-09-24T00:00:00.000Z",
                "type":"event_msg",
                "payload":{"thread_id":session,"turn_id":"receipt-turn","item":{
                    "type":"AgentMessage","id":"receipt-item","content":[{"type":"Text","text":marker}]
                }}
            });
            let handover_text = "Handoff — refresh runtime\nexact selected context";
            let handover_row = serde_json::json!({
                "timestamp":"2026-09-24T00:00:10.000Z",
                "type":"event_msg",
                "payload":{"thread_id":session,"turn_id":"handover-turn","item":{
                    "type":"AgentMessage","id":"handover-item","content":[{"type":"Text","text":handover_text}]
                }}
            });
            let handover_bytes = serde_json::to_vec(&handover_row).unwrap();
            let quoted_tool_row = serde_json::json!({
                "timestamp":"2026-09-24T00:00:05.000Z",
                "type":"event_msg",
                "payload":{"thread_id":session,"turn_id":"handover-turn","item":{
                    "type":"CommandExecution","id":"tool-output","content":"Handoff — refresh runtime\nquoted only"
                }}
            });
            let reference = TranscriptHandoverReference {
                harness_kind: HarnessKind::Codex,
                native_session_id: session.into(),
                native_turn_id: "handover-turn".into(),
                transcript_item_id: "handover-item".into(),
                transcript_role: TranscriptRole::Assistant,
                transcript_title: "Handoff — refresh runtime".into(),
                transcript_timestamp_seconds: 1_790_208_010,
                transcript_record_sha256: format!("{:x}", Sha256::digest(&handover_bytes)),
                handover_selection: HandoverSelection::WholeMessage,
            };
            let rows = if handover_before_receipt {
                vec![handover_row, quoted_tool_row, receipt_row]
            } else {
                vec![receipt_row, quoted_tool_row, handover_row]
            };
            let mut transcript =
                fs::File::create(transcript_root.join(format!("rollout-{session}.jsonl"))).unwrap();
            for row in rows {
                writeln!(transcript, "{row}").unwrap();
            }
            transcript.sync_all().unwrap();
            Self {
                _directory: directory,
                herdr,
                binding,
                receipt,
                reference,
            }
        }
    }

    #[test]
    fn canonical_assistant_handover_after_native_receipt_is_accepted() {
        let fixture = HandoverFixture::canonical(false);
        assert_eq!(
            fixture.herdr.validate_transcript_handover(
                &fixture.binding,
                &fixture.receipt,
                &fixture.reference,
                &RefreshPolicy {
                    maximum_handover_age_seconds: 86_400,
                },
                1_790_208_020,
            ),
            Ok(())
        );
    }

    #[test]
    fn assistant_handover_before_native_receipt_is_rejected() {
        let fixture = HandoverFixture::canonical(true);
        assert_eq!(
            fixture.herdr.validate_transcript_handover(
                &fixture.binding,
                &fixture.receipt,
                &fixture.reference,
                &RefreshPolicy {
                    maximum_handover_age_seconds: 86_400,
                },
                1_790_208_020,
            ),
            Err(RefreshRejection::HandoverBeforeCallerReceipt)
        );
    }

    #[test]
    fn quoted_handover_title_in_tool_output_is_not_an_assistant_handover() {
        let mut fixture = HandoverFixture::canonical(false);
        fixture.reference.transcript_item_id = "tool-output".into();
        assert_eq!(
            fixture.herdr.validate_transcript_handover(
                &fixture.binding,
                &fixture.receipt,
                &fixture.reference,
                &RefreshPolicy {
                    maximum_handover_age_seconds: 86_400,
                },
                1_790_208_020,
            ),
            Err(RefreshRejection::HandoverReferenceInvalid)
        );
    }
}
