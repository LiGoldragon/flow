//! Compaction during continuous work.
//!
//! A flow that keeps working without being given more work may compact.
//! Its compaction raises a warning.
//!
//! A compaction is continuous work when the harness made it on its own: a
//! Claude transcript's `compact_boundary` row whose trigger is `auto`, or a
//! Codex thread's `contextCompaction` item when no Compact was typed into
//! the flow. A Compact Flow typed (`Command.Compact`) is the emergency
//! measure, not continuous work, and raises nothing.
//!
//! Flow learns of a Claude compaction from the flow's own hook: every
//! Report it sends (`SessionStart` follows a compaction at once) has the
//! flow's transcript read from where the last reading stopped. A transcript
//! first read in this Nexus's run is read whole, and only compactions within
//! the committed watch margin at that reading are raised, so a deploy raises
//! none of the compactions made before it. A Codex compaction
//! arrives as its app-server's `item/completed`. Each compaction is
//! recorded once ([`crate::store::compaction`]), so a compaction read twice
//! warns once.
//!
//! The warning is a journal line at warning priority.

use crate::generated::operation::{Operation, Outcome, Record_Data, Record_Data_Context_Data};
use crate::performing::Performs;
use crate::store::compaction::{Compaction, Investigation, RecordsCompactions};
use crate::store::context::RecordsContextSize;
use crate::store::metaflow::SleepsMetaflows;
use crate::store::{NamesLiveFlow, ReadsFlowRows};
use crate::waking::ReadsClaudeDelivery;
use crate::{ConfiguredUse, RunningNexus};
use signal_flow::{Address, Aspect, FlowNode, HarnessKind, Layer};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// How long before the Nexus started a compaction may have been made and
/// still be raised, when a transcript is first read in this run: a Nexus
/// restarted in the middle of a flow's work misses none it made just before.
/// What Flow keeps of the compactions it watches, in memory.
#[derive(Debug)]
pub struct CompactionWatch {
    /// How far each Claude flow's transcript has been read.
    read_to: Mutex<HashMap<String, (PathBuf, u64)>>,
    /// The Codex flows a Compact was typed into: the next compaction of
    /// each is the one asked for.
    requested: Mutex<HashSet<String>>,
}

impl Default for CompactionWatch {
    fn default() -> Self {
        Self {
            read_to: Mutex::default(),
            requested: Mutex::default(),
        }
    }
}

fn compaction_floor(milliseconds: u64) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default();
    now.saturating_sub(milliseconds / 1000).transcript_time()
}

/// A Unix time written as a transcript writes its timestamps.
pub trait WritesTranscriptTime {
    fn transcript_time(&self) -> String;
}

impl WritesTranscriptTime for u64 {
    fn transcript_time(&self) -> String {
        let days = (*self / 86_400) as i64;
        let seconds = *self % 86_400;
        // Civil date from days since 1970-01-01 (Howard Hinnant).
        let shifted = days + 719_468;
        let era = shifted.div_euclid(146_097);
        let of_era = shifted.rem_euclid(146_097);
        let year_of_era = (of_era - of_era / 1_460 + of_era / 36_524 - of_era / 146_096) / 365;
        let day_of_year = of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
        let month_index = (5 * day_of_year + 2) / 153;
        let day = day_of_year - (153 * month_index + 2) / 5 + 1;
        let month = if month_index < 10 {
            month_index + 3
        } else {
            month_index - 9
        };
        let year = year_of_era + era * 400 + i64::from(month <= 2);
        format!(
            "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.000Z",
            seconds / 3_600,
            seconds / 60 % 60,
            seconds % 60
        )
    }
}

/// One compaction a Claude transcript records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeCompaction {
    pub uuid: String,
    /// `auto` when the harness compacted on its own, `manual` when asked.
    pub trigger: String,
    pub timestamp: String,
    pub pre_tokens: Option<i64>,
}

/// A transcript's complete rows after a byte offset, read for compactions.
pub trait ReadsClaudeCompactions {
    /// The main-thread compactions recorded after `offset`, and the offset
    /// past the last complete row read. A file shorter than `offset` was
    /// replaced and is read from its start.
    fn compactions_after(&self, offset: u64) -> Option<(Vec<ClaudeCompaction>, u64)>;
}

impl ReadsClaudeCompactions for Path {
    fn compactions_after(&self, offset: u64) -> Option<(Vec<ClaudeCompaction>, u64)> {
        let mut file = fs::File::open(self).ok()?;
        let length = file.metadata().ok()?.len();
        let start = if length < offset { 0 } else { offset };
        file.seek(SeekFrom::Start(start)).ok()?;
        let mut tail = Vec::new();
        file.read_to_end(&mut tail).ok()?;
        // Only complete rows: a row still being written is read next time.
        let complete = tail
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |last| last + 1);
        let compactions = String::from_utf8_lossy(&tail[..complete])
            .lines()
            .filter(|line| line.contains("\"compact_boundary\""))
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .filter(|row| {
                row.get("subtype").and_then(serde_json::Value::as_str) == Some("compact_boundary")
                    && row.get("isSidechain").and_then(serde_json::Value::as_bool) != Some(true)
            })
            .filter_map(|row| {
                let metadata = row.get("compactMetadata")?;
                Some(ClaudeCompaction {
                    uuid: row.get("uuid")?.as_str()?.to_owned(),
                    trigger: metadata.get("trigger")?.as_str()?.to_owned(),
                    timestamp: row
                        .get("timestamp")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    pre_tokens: metadata
                        .get("preTokens")
                        .and_then(serde_json::Value::as_i64),
                })
            })
            .collect();
        Some((compactions, start + complete as u64))
    }
}

/// What a Codex app-server notification says of a thread's context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexContextSignal {
    /// `thread/tokenUsage/updated`: the input tokens of the thread's last
    /// model call.
    TokenUsage(i64),
    /// `item/completed` of a `contextCompaction` item, by its id.
    Compacted(String),
}

pub trait ReadsCodexContextSignal {
    /// The context signal a JSON-RPC notification carries for `thread`;
    /// None for any other message, or one about another thread.
    fn context_signal(&self, thread: &str) -> Option<CodexContextSignal>;
}

impl ReadsCodexContextSignal for serde_json::Value {
    fn context_signal(&self, thread: &str) -> Option<CodexContextSignal> {
        if self.get("id").is_some() {
            return None;
        }
        let params = self.get("params")?;
        if params.get("threadId").and_then(serde_json::Value::as_str) != Some(thread) {
            return None;
        }
        match self.get("method").and_then(serde_json::Value::as_str)? {
            // A usage carrying no input names no model call: Codex reports
            // one as its thread compacts, and it measures nothing.
            "thread/tokenUsage/updated" => params
                .pointer("/tokenUsage/last/inputTokens")
                .and_then(serde_json::Value::as_i64)
                .filter(|tokens| *tokens > 0)
                .map(CodexContextSignal::TokenUsage),
            "item/completed" => {
                let item = params.get("item")?;
                (item.get("type").and_then(serde_json::Value::as_str) == Some("contextCompaction"))
                    .then(|| item.get("id").and_then(serde_json::Value::as_str))
                    .flatten()
                    .map(|id| CodexContextSignal::Compacted(id.to_owned()))
            }
            _ => None,
        }
    }
}

/// The warning written for a continuous-work compaction.
pub struct CompactionWarning<'warning> {
    pub flow_id: &'warning str,
    pub address: Option<&'warning Address>,
    pub harness_kind: &'warning HarnessKind,
    pub session: &'warning str,
    pub transcript: Option<&'warning Path>,
    pub pre_tokens: Option<i64>,
    pub texts: &'warning meta_signal_flow::Texts,
}

pub trait RendersCompactionWarning {
    fn text(&self) -> String;
}

impl RendersCompactionWarning for CompactionWarning<'_> {
    fn text(&self) -> String {
        let template = match (self.address.is_some(), self.harness_kind, self.transcript) {
            (true, HarnessKind::Claude, Some(_)) => {
                &self
                    .texts
                    .compaction_warning
                    .seated_compaction
                    .seated_compaction_transcript
            }
            (true, HarnessKind::Claude, None) => {
                &self
                    .texts
                    .compaction_warning
                    .seated_compaction
                    .seated_compaction_claude_session
            }
            (true, HarnessKind::Codex, _) => {
                &self
                    .texts
                    .compaction_warning
                    .seated_compaction
                    .seated_compaction_codex_thread
            }
            (false, HarnessKind::Claude, Some(_)) => {
                &self
                    .texts
                    .compaction_warning
                    .unseated_compaction
                    .unseated_compaction_transcript
            }
            (false, HarnessKind::Claude, None) => {
                &self
                    .texts
                    .compaction_warning
                    .unseated_compaction
                    .unseated_compaction_claude_session
            }
            (false, HarnessKind::Codex, _) => {
                &self
                    .texts
                    .compaction_warning
                    .unseated_compaction
                    .unseated_compaction_codex_thread
            }
        };
        use meta_signal_flow::Slot;
        crate::texts::render_template(template, |slot| match slot {
            Slot::LaunchFlowId => Some(self.flow_id.to_owned()),
            Slot::MetaflowAddress => self.address.map(WritesSeat::seat_text),
            Slot::ContextSize => self.pre_tokens.map(|tokens| tokens.to_string()),
            Slot::TranscriptFile => self.transcript.map(|path| path.display().to_string()),
            Slot::SessionId if self.harness_kind == &HarnessKind::Claude => {
                Some(self.session.to_owned())
            }
            Slot::ThreadId if self.harness_kind == &HarnessKind::Codex => {
                Some(self.session.to_owned())
            }
            _ => None,
        })
        .expect("the authored compaction-warning template matches its selected source")
    }
}

/// An Address as a title writes it: `{ Mind flow Secondary }`.
pub trait WritesSeat {
    fn seat_text(&self) -> String;
}

impl WritesSeat for Address {
    fn seat_text(&self) -> String {
        let aspect = match self.aspect {
            Aspect::Psyche => "Psyche",
            Aspect::Mind => "Mind",
            Aspect::Field => "Field",
        };
        let layer = match self.layer {
            Layer::Primary => "Primary",
            Layer::Secondary => "Secondary",
            Layer::Tertiary => "Tertiary",
            Layer::Quaternary => "Quaternary",
        };
        format!("{{ {aspect} {} {layer} }}", self.topic)
    }
}

pub trait WatchesCompactions {
    /// Reads the Claude flow's transcript past the last reading for
    /// compactions its harness made on its own, and raises each.
    fn watch_claude_compactions(&self, flow_id: &str);
    /// The flow's harness goes on in a new session: its transcript is the
    /// new session's, read from its start.
    fn session_rebound(&self, flow_id: &str);
    /// A Codex thread's app-server reported a context signal, under the
    /// immutable configuration selected by the app-server event ingress.
    fn codex_context_signal_configured(
        &self,
        configured: &ConfiguredUse,
        thread: &str,
        signal: CodexContextSignal,
    );
    #[cfg(test)]
    fn codex_context_signal(&self, thread: &str, signal: CodexContextSignal);
    /// A Compact was typed into the flow: its next Codex compaction is the
    /// one asked for.
    fn compaction_requested(&self, flow_id: &str);
    /// Raises a compaction made during continuous work once as a warning.
    fn raise_compaction(
        &self,
        node: &FlowNode,
        boundary: &str,
        pre_tokens: Option<i64>,
        texts: &meta_signal_flow::Texts,
    );
}

impl WatchesCompactions for RunningNexus {
    fn watch_claude_compactions(&self, flow_id: &str) {
        let configured = match self.configured_use() {
            Ok(configured) => configured,
            _ => return,
        };
        let floor = compaction_floor(configured.snapshot.timings.compaction_watch_margin as u64);
        let Ok(Some(node)) = self.store.flow_node(flow_id) else {
            return;
        };
        if node.harness_kind != HarnessKind::Claude || !node.flow_lifecycle.is_live() {
            return;
        }
        let raised = {
            let mut read_to = self
                .compactions
                .read_to
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let known = read_to.get(flow_id).cloned();
            let first = known.is_none();
            let transcript = match known {
                Some((transcript, _)) => Some(transcript),
                None => self.claude_transcript(&node),
            };
            let Some(transcript) = transcript else {
                return;
            };
            let offset = read_to.get(flow_id).map_or(0, |(_, offset)| *offset);
            let Some((found, next)) = transcript.compactions_after(offset) else {
                return;
            };
            read_to.insert(flow_id.to_owned(), (transcript, next));
            found
                .into_iter()
                .filter(|compaction| compaction.trigger == "auto")
                .filter(|compaction| !first || compaction.timestamp >= floor)
                .collect::<Vec<_>>()
        };
        for compaction in raised {
            self.raise_compaction(
                &node,
                &compaction.uuid,
                compaction.pre_tokens,
                &configured.snapshot.texts,
            );
        }
    }

    fn session_rebound(&self, flow_id: &str) {
        self.compactions
            .read_to
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(flow_id);
    }

    fn codex_context_signal_configured(
        &self,
        configured: &ConfiguredUse,
        thread: &str,
        signal: CodexContextSignal,
    ) {
        let Some(node) = self.codex_flow_on_thread(thread) else {
            return;
        };
        match signal {
            CodexContextSignal::TokenUsage(tokens) => {
                if self.perform_configured(
                    configured,
                    Operation::Record(Record_Data::Context(Record_Data_Context_Data {
                        flow_id: node.flow_id.clone(),
                        tokens,
                    })),
                ) != Outcome::Recorded
                {
                    eprintln!(
                        "flow-nexus: flow {} context size not recorded",
                        node.flow_id
                    );
                }
            }
            CodexContextSignal::Compacted(item) => {
                let asked = self
                    .compactions
                    .requested
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .remove(&node.flow_id);
                if !asked {
                    let texts = match self.configured_use() {
                        Ok(configured) => configured.snapshot.texts,
                        _ => return,
                    };
                    let pre_tokens = self.store.context_size(&node.flow_id).ok().flatten();
                    self.raise_compaction(&node, &item, pre_tokens, &texts);
                }
            }
        }
    }

    #[cfg(test)]
    fn codex_context_signal(&self, thread: &str, signal: CodexContextSignal) {
        if let Ok(configured) = self.configured_use() {
            self.codex_context_signal_configured(&configured, thread, signal);
        }
    }

    fn compaction_requested(&self, flow_id: &str) {
        self.compactions
            .requested
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(flow_id.to_owned());
    }

    fn raise_compaction(
        &self,
        node: &FlowNode,
        boundary: &str,
        pre_tokens: Option<i64>,
        texts: &meta_signal_flow::Texts,
    ) {
        let flow_id = node.flow_id.as_str();
        match self.store.compaction_recorded(flow_id, boundary) {
            Ok(false) => {}
            Ok(true) => return,
            Err(error) => {
                eprintln!(
                    "<4>flow-nexus: compaction {boundary} of flow {flow_id} unreadable: {error}"
                );
                return;
            }
        }
        let address = self
            .store
            .metaflow_awake_as(flow_id)
            .ok()
            .flatten()
            .map(|metaflow| metaflow.address);
        let transcript = match node.harness_kind {
            HarnessKind::Claude => self.claude_transcript(node),
            HarnessKind::Codex => None,
        };
        let warning = CompactionWarning {
            flow_id,
            address: address.as_ref(),
            harness_kind: &node.harness_kind,
            session: &node.session_id,
            transcript: transcript.as_deref(),
            pre_tokens,
            texts,
        }
        .text();
        let compaction = Compaction {
            flow_id: flow_id.to_owned(),
            boundary: boundary.to_owned(),
            address: address.clone(),
            investigation: Investigation::NotConfigured,
        };
        match self.store.record_compaction(compaction.clone()) {
            Ok(true) => {}
            Ok(false) => return,
            Err(error) => {
                eprintln!(
                    "<4>flow-nexus: compaction {boundary} of flow {flow_id} not recorded: {error}"
                );
                return;
            }
        }
        eprintln!("<4>flow-nexus: WARNING {warning}");
    }
}

/// The live Codex flow on a thread, when there is exactly one.
trait FindsCodexFlow {
    fn codex_flow_on_thread(&self, thread: &str) -> Option<FlowNode>;
}

impl FindsCodexFlow for RunningNexus {
    fn codex_flow_on_thread(&self, thread: &str) -> Option<FlowNode> {
        use crate::codex_turns::SettlesRestingCodexSeats;
        self.codex_flow_on(thread)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ClaudeCompaction, CodexContextSignal, CompactionWarning, ReadsClaudeCompactions,
        ReadsCodexContextSignal, RendersCompactionWarning, WritesTranscriptTime, compaction_floor,
    };
    use signal_flow::{Address, Aspect, HarnessKind, Layer};
    use std::io::Write;

    fn boundary(uuid: &str, trigger: &str, timestamp: &str) -> String {
        format!(
            "{{\"type\":\"system\",\"subtype\":\"compact_boundary\",\"uuid\":\"{uuid}\",\"timestamp\":\"{timestamp}\",\"isSidechain\":false,\"compactMetadata\":{{\"trigger\":\"{trigger}\",\"preTokens\":219141}}}}\n"
        )
    }

    #[test]
    fn each_first_read_can_use_the_current_watch_margin() {
        assert!(compaction_floor(0) > compaction_floor(60_000));
    }

    #[test]
    fn a_transcript_is_read_for_compactions_from_where_the_last_reading_stopped() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("session.jsonl");
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(b"{\"type\":\"user\",\"message\":{\"content\":\"go\"}}\n")
            .unwrap();
        file.write_all(boundary("b1", "auto", "2026-10-19T20:00:00.000Z").as_bytes())
            .unwrap();
        file.write_all(boundary("b2", "manual", "2026-10-19T20:05:00.000Z").as_bytes())
            .unwrap();
        // A row still being written is not read.
        file.write_all(b"{\"type\":\"system\",\"subtype\":\"compact_bou")
            .unwrap();
        let (found, offset) = path.compactions_after(0).unwrap();
        assert_eq!(
            found,
            vec![
                ClaudeCompaction {
                    uuid: "b1".into(),
                    trigger: "auto".into(),
                    timestamp: "2026-10-19T20:00:00.000Z".into(),
                    pre_tokens: Some(219141),
                },
                ClaudeCompaction {
                    uuid: "b2".into(),
                    trigger: "manual".into(),
                    timestamp: "2026-10-19T20:05:00.000Z".into(),
                    pre_tokens: Some(219141),
                },
            ]
        );
        file.write_all(b"ndary\"}\n").unwrap();
        file.write_all(boundary("b3", "auto", "2026-10-19T21:00:00.000Z").as_bytes())
            .unwrap();
        let (found, _) = path.compactions_after(offset).unwrap();
        assert_eq!(
            found
                .iter()
                .map(|found| found.uuid.as_str())
                .collect::<Vec<_>>(),
            ["b3"]
        );
        // A side chain's compaction is not the flow's.
        let side = directory.path().join("side.jsonl");
        std::fs::write(
            &side,
            boundary("s1", "auto", "2026-10-19T20:00:00.000Z")
                .replace("\"isSidechain\":false", "\"isSidechain\":true"),
        )
        .unwrap();
        assert!(side.compactions_after(0).unwrap().0.is_empty());
    }

    #[test]
    fn a_unix_time_is_written_as_a_transcript_writes_it() {
        assert_eq!(0_u64.transcript_time(), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            1_792_368_000_u64.transcript_time(),
            "2026-10-19T00:00:00.000Z"
        );
        assert_eq!(
            1_709_210_096_u64.transcript_time(),
            "2024-02-29T12:34:56.000Z"
        );
        // Written times order as the times do.
        assert!(1_792_368_000_u64.transcript_time().as_str() < "2026-10-19T20:00:00.000Z");
    }

    #[test]
    fn a_codex_notification_names_token_usage_and_compaction_of_its_thread_only() {
        let usage = serde_json::json!({
            "method": "thread/tokenUsage/updated",
            "params": {"threadId": "t1", "turnId": "u1", "tokenUsage": {
                "total": {"totalTokens": 900, "inputTokens": 800},
                "last": {"totalTokens": 160500, "inputTokens": 160000},
                "modelContextWindow": 272000}}
        });
        assert_eq!(
            usage.context_signal("t1"),
            Some(CodexContextSignal::TokenUsage(160000))
        );
        assert_eq!(usage.context_signal("t2"), None);
        // Witnessed live (Field core Secondary 42265e, 2026-10-19 20:35): a
        // usage with no input arrives around a compaction; it is not a
        // measurement.
        let emptied = serde_json::json!({
            "method": "thread/tokenUsage/updated",
            "params": {"threadId": "t1", "turnId": "u1", "tokenUsage": {
                "total": {"totalTokens": 900, "inputTokens": 800},
                "last": {"totalTokens": 0, "inputTokens": 0},
                "modelContextWindow": 272000}}
        });
        assert_eq!(emptied.context_signal("t1"), None);
        let compacted = serde_json::json!({
            "method": "item/completed",
            "params": {"threadId": "t1", "turnId": "u1",
                       "item": {"type": "contextCompaction", "id": "item_7"}}
        });
        assert_eq!(
            compacted.context_signal("t1"),
            Some(CodexContextSignal::Compacted("item_7".into()))
        );
        let message = serde_json::json!({
            "method": "item/completed",
            "params": {"threadId": "t1", "item": {"type": "agentMessage", "id": "item_8"}}
        });
        assert_eq!(message.context_signal("t1"), None);
    }

    #[test]
    fn the_warning_names_the_flow_and_its_seat() {
        let address = Address {
            aspect: Aspect::Mind,
            topic: "flow".into(),
            layer: Layer::Secondary,
        };
        let transcript = std::path::PathBuf::from("/home/li/.claude/projects/p/s.jsonl");
        let text = CompactionWarning {
            flow_id: "5a4d0b",
            address: Some(&address),
            harness_kind: &HarnessKind::Claude,
            session: "s",
            transcript: Some(&transcript),
            pre_tokens: Some(219141),
            texts: &crate::store::texts::seed_texts().expect("texts seed"),
        }
        .text();
        assert!(text.starts_with(
            "Flow warning: flow 5a4d0b ({ Mind flow Secondary }) compacted its context at 219141 tokens while it kept working without being given more work."
        ));
        assert!(text.contains("Its transcript is /home/li/.claude/projects/p/s.jsonl."));
        assert!(!text.contains("Investigate"));
    }

    #[test]
    fn seated_and_unseated_warnings_select_each_source_with_raw_values() {
        let texts = crate::store::texts::seed_texts().expect("texts seed");
        let address = Address {
            aspect: Aspect::Mind,
            topic: "flow".into(),
            layer: Layer::Secondary,
        };
        let transcript = std::path::PathBuf::from("/raw/transcript.jsonl");
        for seated in [false, true] {
            let address = seated.then_some(&address);
            let transcript_text = CompactionWarning {
                flow_id: "f",
                address,
                harness_kind: &HarnessKind::Claude,
                session: "raw-session",
                transcript: Some(&transcript),
                pre_tokens: Some(0),
                texts: &texts,
            }
            .text();
            assert!(transcript_text.contains(" at 0 tokens"));
            assert!(transcript_text.ends_with("Its transcript is /raw/transcript.jsonl."));

            let claude = CompactionWarning {
                flow_id: "f",
                address,
                harness_kind: &HarnessKind::Claude,
                session: "raw-session",
                transcript: None,
                pre_tokens: None,
                texts: &texts,
            }
            .text();
            assert!(!claude.contains(" tokens"));
            assert!(claude.ends_with("Its Claude session is raw-session."));

            let codex_with_transcript = CompactionWarning {
                flow_id: "f",
                address,
                harness_kind: &HarnessKind::Codex,
                session: "raw-thread",
                transcript: Some(&transcript),
                pre_tokens: None,
                texts: &texts,
            }
            .text();
            assert!(codex_with_transcript.ends_with("Its Codex thread is raw-thread."));
            assert!(!codex_with_transcript.contains("/raw/transcript"));
        }
    }
}
