//! `flow-nexus-hook`: the Claude Code command hook Flow writes into every
//! Claude flow it launches. Claude Code runs it on SessionStart, PostToolUse,
//! UserPromptSubmit, Stop and SessionEnd with the event as JSON on stdin; it turns the
//! event into one `Report.{ FlowId Option<Process> Event }` and sends it,
//! through the client the `flow-nexus` CLI speaks Signal with, to the socket
//! its typed configuration names: the socket of the place whose Nexus
//! launched the flow.
//!
//!   SessionStart     -> Report.{ «FLOW_ID» BIRTH Started.Some.«session_id» }
//!   PostToolUse      -> Report.{ «FLOW_ID» BIRTH ToolUsed.«tool_name» }
//!   UserPromptSubmit -> Report.{ «FLOW_ID» BIRTH PromptSubmitted.{ Some.«session_id» Some.«prompt_id» SHA256 } }
//!   Stop             -> Report.{ «FLOW_ID» BIRTH Stopped.{ Some.«session_id» Some.«transcript_path» Some.«last_assistant_message» [ «background_tasks» ] } }
//!   SessionEnd       -> Report.{ «FLOW_ID» BIRTH Ended.Some.«session_id» }
//!   SessionEnd clear -> Report.{ «FLOW_ID» BIRTH Cleared.Some.«session_id» }
//!
//! BIRTH names the harness that ran the hook, `Some.{ pid start }`: the
//! nearest process above the hook whose command line names the event's
//! session after `--session-id` or `--resume`, or whose Claude session
//! record (`<Claude home>/sessions/<pid>.json`, at the start it runs with
//! now) names it; `None` when no ancestor does. An event names the harness
//! it came from in its own data, so a resumed seat's events are told from a
//! late hook of the harness it replaced, which shares its session.
//!
//! A SessionEnd whose reason is `clear` is a session change, not an exit:
//! the harness goes on in the session the SessionStart after it names.
//!
//! A Stop carries the background work Claude Code names in its input's
//! `background_tasks`, one `{ id type status description }` each, `[]` when
//! nothing is in flight: a Stop with work in flight is no rest, and the Stop
//! Claude Code ends its turn with once that work has ended is. A Stop whose
//! input has no `background_tasks`, or a task without those four strings,
//! does not decode: it is refused, nothing is reported, and the hook exits
//! 1 with the typed refusal on stderr, which Claude Code shows as a
//! non-blocking hook error. A Stop that does not decode is never read as
//! rest.
//!
//! A Stop and a submitted prompt carry the native identity Claude Code gave
//! the hook, so the Nexus acts on them only when they are the flow's own
//! registered session; a field the input does not carry is `None`. A
//! submitted prompt travels as the SHA-256 of the text Claude Code took
//! (its paste wrapper taken off), never as the text: one argument holds at
//! most 128 KiB on Linux, and the digest is an exact match of the whole
//! prompt.
//!
//! The FlowId is the `FLOW_ID` the flow claimed, read from the harness's
//! environment, never derived from the harness's session id (ruling 12 of
//! flow f1c841). With no FLOW_ID, or on any other event, it reports
//! nothing.
//!
//! It never blocks the harness: it exits 0, or 1 for a refused Stop, never
//! 2, writes nothing on stdout (which UserPromptSubmit would add to the
//! model's context), and writes one tab-separated line on stderr (event,
//! datom, the call's exit code as the CLI would give it, 0 or 2, and the
//! reply or the error; for a refused Stop, the refusal) for whoever reads
//! it.

use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use flow::client::{CallsFlowNexus, FlowClient, TextualizesFlowReply};
use flow::{HookConfiguration, Invocation};
use flow_native::{DigestsPrompt, ReadsClaudePaste};
use protos::{Compactable, Protosizable, ReaderBudget};
use signal_flow::{
    BackgroundTask, BackgroundTaskKind, BackgroundWork, Event, NativePrompt, NativeStop, Query,
    Report_Data,
};
use std::{env, io::Read, process::ExitCode};

/// Local diagnostics carry the metadata of a Report, independently of its transport.
#[derive(datom_codec::Datomizable, datom_codec::Composing)]
struct HookReportMetadata {
    flow_id: signal_flow::FlowId,
    process_option: Option<signal_flow::Process>,
    hook_event_metadata: HookEventMetadata,
}

#[derive(datom_codec::Datomizable, datom_codec::Composing)]
enum HookEventMetadata {
    Started(Option<signal_flow::NativeSessionId>),
    ToolUsed(String),
    Stopped(HookStopMetadata),
    PromptSubmitted(NativePrompt),
    Ended(Option<signal_flow::NativeSessionId>),
    Cleared(Option<signal_flow::NativeSessionId>),
}

#[derive(datom_codec::Datomizable, datom_codec::Composing)]
struct HookStopMetadata {
    native_session_id_option: Option<signal_flow::NativeSessionId>,
    transcript_path_option: Option<signal_flow::TranscriptPath>,
    background_work: BackgroundWork,
}

trait DescribesHookMetadata {
    fn metadata(&self) -> HookEventMetadata;
}

impl DescribesHookMetadata for Event {
    fn metadata(&self) -> HookEventMetadata {
        match self {
            Self::Started(session) => HookEventMetadata::Started(session.clone()),
            Self::ToolUsed(tool) => HookEventMetadata::ToolUsed(tool.clone()),
            Self::Stopped(stop) => HookEventMetadata::Stopped(HookStopMetadata {
                native_session_id_option: stop.native_session_id_option.clone(),
                transcript_path_option: stop.transcript_path_option.clone(),
                background_work: stop.background_work.clone(),
            }),
            Self::PromptSubmitted(prompt) => HookEventMetadata::PromptSubmitted(prompt.clone()),
            Self::Ended(session) => HookEventMetadata::Ended(session.clone()),
            Self::Cleared(session) => HookEventMetadata::Cleared(session.clone()),
        }
    }
}

/// One hook invocation: the event Claude Code wrote, the flow it ran in,
/// and the birth of the harness that ran it.
struct HarnessHook {
    input: serde_json::Value,
    flow_id: Option<String>,
    configuration: Option<HookConfiguration>,
    harness: Option<flow_ethos::Process>,
}

/// What the kernel and Claude Code show of the processes above the hook.
trait ReadsProcessTable {
    /// A process's parent and its start time, from `/proc/<pid>/stat`.
    fn parent_and_start(&self, pid: u32) -> Option<(u32, i64)>;
    /// A process's command line, argument by argument.
    fn arguments(&self, pid: u32) -> Vec<String>;
    /// The session Claude Code's record of `pid` names, when the record
    /// names that process at `started`.
    fn claude_session(&self, pid: u32, started: i64) -> Option<String>;
}

/// The kernel's `/proc` and the session records under a Claude home.
struct ProcessTable {
    claude_home: std::path::PathBuf,
}

impl ReadsProcessTable for ProcessTable {
    fn parent_and_start(&self, pid: u32) -> Option<(u32, i64)> {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let (_, fields) = stat.rsplit_once(") ")?;
        let fields = fields.split_whitespace().collect::<Vec<_>>();
        Some((fields.get(1)?.parse().ok()?, fields.get(19)?.parse().ok()?))
    }

    fn arguments(&self, pid: u32) -> Vec<String> {
        std::fs::read(format!("/proc/{pid}/cmdline"))
            .map(|bytes| {
                bytes
                    .split(|byte| *byte == 0)
                    .filter(|argument| !argument.is_empty())
                    .map(|argument| String::from_utf8_lossy(argument).into_owned())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn claude_session(&self, pid: u32, started: i64) -> Option<String> {
        let text = std::fs::read_to_string(
            self.claude_home
                .join("sessions")
                .join(format!("{pid}.json")),
        )
        .ok()?;
        let record: serde_json::Value = serde_json::from_str(&text).ok()?;
        let recorded_start = match record.get("procStart")? {
            serde_json::Value::String(start) => start.parse::<i64>().ok()?,
            serde_json::Value::Number(start) => start.as_i64()?,
            _ => return None,
        };
        (record.get("pid").and_then(serde_json::Value::as_u64) == Some(u64::from(pid))
            && recorded_start == started)
            .then(|| record.get("sessionId")?.as_str().map(str::to_owned))
            .flatten()
            .filter(|session| !session.is_empty())
    }
}

/// The harness that runs `session`, found above the process `below`.
trait FindsHarness {
    fn harness_above(&self, below: u32, session: &str) -> Option<flow_ethos::Process>;
}

impl<Table: ReadsProcessTable> FindsHarness for Table {
    fn harness_above(&self, below: u32, session: &str) -> Option<flow_ethos::Process> {
        const ANCESTRY_LIMIT: usize = 64;
        let (mut pid, _) = self.parent_and_start(below)?;
        for _ in 0..ANCESTRY_LIMIT {
            if pid <= 1 {
                return None;
            }
            let (parent, started) = self.parent_and_start(pid)?;
            let named = self.arguments(pid).windows(2).any(|pair| {
                (pair[0] == "--session-id" || pair[0] == "--resume") && pair[1] == session
            }) || self.claude_session(pid, started).as_deref() == Some(session);
            if named {
                return Some(flow_ethos::Process {
                    pid: flow_ethos::Pid(i64::from(pid)),
                    started: flow_ethos::Started(started),
                });
            }
            pid = parent;
        }
        None
    }
}

/// Why a Stop's input does not decode. It is never taken for the flow's
/// rest: nothing is reported for it.
#[derive(Debug, PartialEq, Eq)]
enum StopRefusal {
    /// The input carries no `background_tasks` array.
    BackgroundTasksAbsent,
    /// The task at this place lacks one of its id, type, status and
    /// description strings.
    BackgroundTaskMalformed(usize),
}

/// What the hook does with one event.
#[derive(Debug, PartialEq, Eq)]
enum HookCall {
    Report(Box<Query>),
    Nothing(&'static str),
    Refused(StopRefusal),
}

/// A refusal written as the datom it names.
trait NamesRefusal {
    fn datom(&self) -> String;
}

impl NamesRefusal for StopRefusal {
    fn datom(&self) -> String {
        match self {
            Self::BackgroundTasksAbsent => "Refused.BackgroundTasksAbsent".into(),
            Self::BackgroundTaskMalformed(place) => {
                format!("Refused.BackgroundTaskMalformed.{place}")
            }
        }
    }
}

/// The background work a Stop's input names.
trait ReadsBackgroundWork {
    fn background_work(&self) -> Result<BackgroundWork, StopRefusal>;
}

/// The feature a background task's `type` label names.
trait NamesBackgroundTaskKind {
    fn background_task_kind(&self) -> BackgroundTaskKind;
}

impl NamesBackgroundTaskKind for str {
    fn background_task_kind(&self) -> BackgroundTaskKind {
        match self {
            "shell" => BackgroundTaskKind::Shell,
            "subagent" => BackgroundTaskKind::Subagent,
            "monitor" => BackgroundTaskKind::Monitor,
            "workflow" => BackgroundTaskKind::Workflow,
            "teammate" => BackgroundTaskKind::Teammate,
            "cloud session" => BackgroundTaskKind::CloudSession,
            "MCP task" => BackgroundTaskKind::McpTask,
            other => BackgroundTaskKind::Other(other.to_owned()),
        }
    }
}

trait ReadsHookInput: Sized {
    fn from_environment() -> Self;
}

trait ChoosesHookCall {
    fn event_name(&self) -> String;
    fn call(&self) -> HookCall;
}

trait ReadsHookField {
    fn optional(&self, field: &str) -> Option<String>;
}

trait ReportsThroughCli {
    /// The stderr line, and whether the event was refused.
    fn run(&self) -> (String, bool);
}

impl ReadsHookInput for HarnessHook {
    fn from_environment() -> Self {
        let mut text = String::new();
        let _ = std::io::stdin().read_to_string(&mut text);
        let input: serde_json::Value =
            serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
        let claude_home = env::var_os("CLAUDE_CONFIG_DIR")
            .map(std::path::PathBuf::from)
            .or_else(|| env::var_os("HOME").map(|home| std::path::Path::new(&home).join(".claude")))
            .unwrap_or_default();
        let harness = input
            .get("session_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|session| {
                ProcessTable { claude_home }.harness_above(std::process::id(), session)
            });
        Self {
            input,
            harness,
            flow_id: env::var("FLOW_ID")
                .ok()
                .filter(|flow_id| !flow_id.is_empty()),
            configuration: env::args().nth(1).and_then(|source| {
                let mut budget = Budget {
                    remaining: 65_536,
                    reader: ReaderBudget::try_from(65_536).expect("a constant reader budget"),
                    depth: 0,
                    maximum_depth: 1_024,
                };
                Potential::<HookConfiguration>::from(source)
                    .actualize(&mut budget)
                    .ok()
            }),
        }
    }
}

impl ReadsHookField for HarnessHook {
    fn optional(&self, field: &str) -> Option<String> {
        self.input
            .get(field)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
    }
}

impl ReadsBackgroundWork for HarnessHook {
    fn background_work(&self) -> Result<BackgroundWork, StopRefusal> {
        let Some(tasks) = self
            .input
            .get("background_tasks")
            .and_then(serde_json::Value::as_array)
        else {
            return Err(StopRefusal::BackgroundTasksAbsent);
        };
        tasks
            .iter()
            .enumerate()
            .map(|(place, task)| {
                let field = |name: &str| task.get(name).and_then(serde_json::Value::as_str);
                match (
                    field("id"),
                    field("type"),
                    field("status"),
                    field("description"),
                ) {
                    (Some(id), Some(kind), Some(status), Some(description)) => Ok(BackgroundTask {
                        background_task_id: signal_flow::BackgroundTaskId(id.to_owned()),
                        background_task_kind: kind.background_task_kind(),
                        background_task_status: signal_flow::BackgroundTaskStatus(
                            status.to_owned(),
                        ),
                        description: signal_flow::Description(description.to_owned()),
                    }),
                    _ => Err(StopRefusal::BackgroundTaskMalformed(place)),
                }
            })
            .collect::<Result<Vec<_>, _>>()
            .map(BackgroundWork)
    }
}

impl ChoosesHookCall for HarnessHook {
    fn event_name(&self) -> String {
        self.input
            .get("hook_event_name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("-")
            .to_owned()
    }

    fn call(&self) -> HookCall {
        let Some(flow_id) = &self.flow_id else {
            return HookCall::Nothing("no FLOW_ID in the harness environment");
        };
        let event = match self.event_name().as_str() {
            "SessionStart" => Event::Started(
                self.optional("session_id")
                    .map(signal_flow::NativeSessionId),
            ),
            "PostToolUse" => match self
                .input
                .get("tool_name")
                .and_then(serde_json::Value::as_str)
            {
                Some(tool_name) => Event::ToolUsed(tool_name.to_owned()),
                None => return HookCall::Nothing("PostToolUse without a tool_name"),
            },
            "UserPromptSubmit" => {
                match self.input.get("prompt").and_then(serde_json::Value::as_str) {
                    Some(prompt) => Event::PromptSubmitted(NativePrompt {
                        native_session_id_option: self
                            .optional("session_id")
                            .map(signal_flow::NativeSessionId),
                        prompt_id_option: self.optional("prompt_id").map(signal_flow::PromptId),
                        prompt_sha256: signal_flow::PromptSha256(
                            prompt.submitted_claude_text().prompt_sha256(),
                        ),
                    }),
                    None => return HookCall::Nothing("UserPromptSubmit without a prompt"),
                }
            }
            // A session /clear ended: the harness goes on in a new
            // session, so the flow has not exited.
            "SessionEnd"
                if self.input.get("reason").and_then(serde_json::Value::as_str)
                    == Some("clear") =>
            {
                Event::Cleared(
                    self.optional("session_id")
                        .map(signal_flow::NativeSessionId),
                )
            }
            // The session ends: the flow has exited, whether or not
            // anything looks at its pane.
            "SessionEnd" => Event::Ended(
                self.optional("session_id")
                    .map(signal_flow::NativeSessionId),
            ),
            "Stop" => match self.background_work() {
                Ok(background_work) => Event::Stopped(NativeStop {
                    native_session_id_option: self
                        .optional("session_id")
                        .map(signal_flow::NativeSessionId),
                    transcript_path_option: self
                        .optional("transcript_path")
                        .map(signal_flow::TranscriptPath),
                    last_assistant_message_option: signal_flow::LastAssistantMessageOption(
                        self.optional("last_assistant_message"),
                    ),
                    background_work,
                }),
                Err(refusal) => return HookCall::Refused(refusal),
            },
            _ => return HookCall::Nothing("no Report for this event"),
        };
        HookCall::Report(Box::new(Query::Report(Report_Data {
            flow_id: signal_flow::FlowId(flow_id.clone()),
            process_option: self.harness.clone(),
            event,
        })))
    }
}

impl ReportsThroughCli for HarnessHook {
    fn run(&self) -> (String, bool) {
        let query = match self.call() {
            HookCall::Report(query) => *query,
            HookCall::Nothing(reason) => {
                return (format!("{}\t-\t-\t{reason}", self.event_name()), false);
            }
            HookCall::Refused(refusal) => {
                return (
                    format!("{}\t-\t-\t{}", self.event_name(), refusal.datom()),
                    true,
                );
            }
        };
        let Query::Report(report) = &query else {
            unreachable!("a hook call constructs only a Report");
        };
        let metadata = HookReportMetadata {
            flow_id: report.flow_id.clone(),
            process_option: report.process_option.clone(),
            hook_event_metadata: report.event.metadata(),
        };
        let datom = metadata.datomize(vec![]).protosize().compact();
        let Some(configuration) = &self.configuration else {
            return (
                format!(
                    "{}\t{datom}\t-\tinvalid hook configuration",
                    self.event_name()
                ),
                false,
            );
        };
        let invocation = Invocation {
            ordinary_socket_path: configuration.ordinary_socket_path.clone(),
            query,
        };
        let mut replies = Vec::new();
        let line = match FlowClient.call(&invocation, &mut |reply| {
            replies.push(FlowClient.textualize_reply(&reply))
        }) {
            Ok(()) => format!("{}\t{datom}\t0\t{}", self.event_name(), replies.join(" ")),
            Err(error) => format!("{}\t{datom}\t2\t{error}", self.event_name()),
        };
        (line, false)
    }
}

fn main() -> ExitCode {
    let (line, refused) = HarnessHook::from_environment().run();
    eprintln!("{line}");
    // A refused Stop is a non-blocking hook error Claude Code shows; 2 would
    // keep the turn going, and is never used.
    if refused {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ChoosesHookCall, FindsHarness, HarnessHook, HookCall, ReadsProcessTable, ReportsThroughCli,
        StopRefusal,
    };
    use datom_codec::{Actualizing, Budget, Potential};
    use protos::ReaderBudget;

    fn report(source: String) -> HookCall {
        let mut budget = Budget {
            remaining: 65_536,
            reader: ReaderBudget::try_from(65_536).expect("a constant reader budget"),
            depth: 0,
            maximum_depth: 1_024,
        };
        HookCall::Report(Box::new(
            Potential::<signal_flow::Query>::from(source)
                .actualize(&mut budget)
                .expect("fixture query"),
        ))
    }

    fn hook(input: serde_json::Value, flow_id: Option<&str>) -> HarnessHook {
        HarnessHook {
            input,
            flow_id: flow_id.map(str::to_owned),
            configuration: Some(flow::HookConfiguration {
                ordinary_socket_path: flow_ethos::OrdinarySocketPath("unused".into()),
            }),
            harness: None,
        }
    }

    /// A process tree: pid, its parent, its start, its command line, and
    /// the session its Claude record names.
    struct Table(Vec<(u32, u32, i64, &'static [&'static str], Option<&'static str>)>);

    impl ReadsProcessTable for Table {
        fn parent_and_start(&self, pid: u32) -> Option<(u32, i64)> {
            self.0
                .iter()
                .find(|row| row.0 == pid)
                .map(|row| (row.1, row.2))
        }
        fn arguments(&self, pid: u32) -> Vec<String> {
            self.0
                .iter()
                .find(|row| row.0 == pid)
                .map(|row| {
                    row.3
                        .iter()
                        .map(|argument| (*argument).to_owned())
                        .collect()
                })
                .unwrap_or_default()
        }
        fn claude_session(&self, pid: u32, started: i64) -> Option<String> {
            self.0
                .iter()
                .find(|row| row.0 == pid && row.2 == started)
                .and_then(|row| row.4.map(str::to_owned))
        }
    }

    /// The hook names the nearest process above it that runs the event's
    /// session: by `--resume` or `--session-id` in its command line, or by
    /// its Claude session record; none when no ancestor runs it.
    #[test]
    fn the_hook_names_the_harness_that_runs_its_session() {
        let resumed = Table(vec![
            (900, 800, 9_000, &["flow-nexus-hook"], None),
            (800, 700, 8_000, &["sh", "-c", "flow-nexus-hook"], None),
            (
                700,
                600,
                7_000,
                &["claude", "--resume", "s1", "--model", "m"],
                None,
            ),
            (600, 1, 6_000, &["herdr"], None),
        ]);
        assert_eq!(
            resumed.harness_above(900, "s1"),
            Some(flow_ethos::Process {
                pid: flow_ethos::Pid(700),
                started: flow_ethos::Started(7_000),
            })
        );
        assert_eq!(resumed.harness_above(900, "s2"), None);
        let recorded = Table(vec![
            (900, 700, 9_000, &["flow-nexus-hook"], None),
            (700, 1, 7_100, &["node", "claude"], Some("s1")),
        ]);
        assert_eq!(
            recorded.harness_above(900, "s1"),
            Some(flow_ethos::Process {
                pid: flow_ethos::Pid(700),
                started: flow_ethos::Started(7_100),
            })
        );
    }

    /// The Report carries the harness's birth as the hook found it.
    #[test]
    fn a_report_names_its_harness_birth() {
        let mut named = hook(
            serde_json::json!({"hook_event_name": "SessionEnd", "session_id": "s",
                "reason": "logout"}),
            Some("5a4d0b"),
        );
        named.harness = Some(flow_ethos::Process {
            pid: flow_ethos::Pid(700),
            started: flow_ethos::Started(7_000),
        });
        assert_eq!(
            named.call(),
            report("Report.{ «5a4d0b» Some.{ 700 7000 } Ended.Some.«s» }".into())
        );
    }

    /// The datoms are the ones signal-flow 9.0.0's Report reads, written
    /// out here from ruling 12, not computed through the hook.
    #[test]
    fn each_harness_event_becomes_one_report_of_the_claimed_flow() {
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "SessionStart", "session_id": "s"}),
                Some("5a4d0b")
            )
            .call(),
            report("Report.{ «5a4d0b» None Started.Some.«s» }".into())
        );
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "PostToolUse", "tool_name": "Bash"}),
                Some("5a4d0b")
            )
            .call(),
            report("Report.{ «5a4d0b» None ToolUsed.«Bash» }".into())
        );
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "Stop", "prompt_id": "p",
                    "session_id": "s", "transcript_path": "/t/s.jsonl",
                    "last_assistant_message": "answer", "background_tasks": []}),
                Some("5a4d0b")
            )
            .call(),
            report(
                "Report.{ «5a4d0b» None Stopped.{ Some.«s» Some.«/t/s.jsonl» Some.«answer» [] } }"
                    .into()
            )
        );
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "Stop", "background_tasks": []}),
                Some("5a4d0b")
            )
            .call(),
            report("Report.{ «5a4d0b» None Stopped.{ None None None [] } }".into())
        );
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "Stop",
                    "last_assistant_message": "", "background_tasks": []}),
                Some("5a4d0b")
            )
            .call(),
            report("Report.{ «5a4d0b» None Stopped.{ None None Some.«» [] } }".into())
        );
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "SessionEnd", "session_id": "s",
                    "reason": "prompt_input_exit"}),
                Some("5a4d0b")
            )
            .call(),
            report("Report.{ «5a4d0b» None Ended.Some.«s» }".into())
        );
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "SessionEnd", "session_id": "s",
                    "reason": "logout"}),
                Some("5a4d0b")
            )
            .call(),
            report("Report.{ «5a4d0b» None Ended.Some.«s» }".into())
        );
    }

    /// A Stop carries every background task Claude Code names in flight, a
    /// monitor as any other, each by the feature that created it; a label
    /// none names is kept as written.
    #[test]
    fn a_stop_carries_its_background_work_a_monitor_included() {
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "Stop", "session_id": "s",
                "transcript_path": "/t/s.jsonl", "background_tasks": [
                    {"id": "b1", "type": "shell", "status": "running",
                        "description": "tail logs", "command": "tail -f log"},
                    {"id": "a1", "type": "subagent", "status": "running",
                        "description": "review", "agent_type": "Explore"},
                    {"id": "m1", "type": "monitor", "status": "running",
                        "description": "watch CI", "server": "ci", "tool": "watch"},
                    {"id": "x1", "type": "MCP task", "status": "pending",
                        "description": "fetch", "server": "s", "tool": "t"},
                    {"id": "y1", "type": "later kind", "status": "running",
                        "description": "new"}
                ]}),
                Some("5a4d0b")
            )
            .call(),
            report(
                "Report.{ «5a4d0b» None Stopped.{ Some.«s» Some.«/t/s.jsonl» None [ \
                 { «b1» Shell «running» «tail logs» } \
                 { «a1» Subagent «running» «review» } \
                 { «m1» Monitor «running» «watch CI» } \
                 { «x1» McpTask «pending» «fetch» } \
                 { «y1» Other.«later kind» «running» «new» } ] } }"
                    .into()
            )
        );
    }

    /// A Stop whose input names no background work, as a Claude Code
    /// before 2.1.294 writes it, or names a task without its four strings,
    /// does not decode: it is refused, nothing is reported, and the hook's
    /// line names the refusal and asks for the error exit.
    #[test]
    fn an_undecodable_stop_is_refused_visibly() {
        let absent = hook(
            serde_json::json!({"hook_event_name": "Stop", "session_id": "s",
                "transcript_path": "/t/s.jsonl"}),
            Some("5a4d0b"),
        );
        assert_eq!(
            absent.call(),
            HookCall::Refused(StopRefusal::BackgroundTasksAbsent)
        );
        assert_eq!(
            absent.run(),
            ("Stop\t-\t-\tRefused.BackgroundTasksAbsent".to_owned(), true)
        );
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "Stop", "background_tasks": {}}),
                Some("5a4d0b")
            )
            .call(),
            HookCall::Refused(StopRefusal::BackgroundTasksAbsent)
        );
        let malformed = hook(
            serde_json::json!({"hook_event_name": "Stop", "background_tasks": [
                {"id": "m1", "type": "monitor", "status": "running", "description": "watch"},
                {"id": "a1", "type": "subagent", "description": "review"}
            ]}),
            Some("5a4d0b"),
        );
        assert_eq!(
            malformed.call(),
            HookCall::Refused(StopRefusal::BackgroundTaskMalformed(1))
        );
        assert_eq!(
            malformed.run(),
            (
                "Stop\t-\t-\tRefused.BackgroundTaskMalformed.1".to_owned(),
                true
            )
        );
    }

    /// A session /clear ends is a session change: its SessionEnd reports
    /// Cleared, never Ended, and the SessionStart after it names the new
    /// session.
    #[test]
    fn a_cleared_session_reports_cleared_and_its_successor_session_started() {
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "SessionEnd", "session_id": "s1",
                    "reason": "clear"}),
                Some("5a4d0b")
            )
            .call(),
            report("Report.{ «5a4d0b» None Cleared.Some.«s1» }".into())
        );
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "SessionStart", "session_id": "s2",
                    "source": "clear"}),
                Some("5a4d0b")
            )
            .call(),
            report("Report.{ «5a4d0b» None Started.Some.«s2» }".into())
        );
    }

    /// A submitted prompt is reported as the digest of the text Claude Code
    /// took: the exact body of its paste wrapper, never the wrapper.
    #[test]
    fn a_submitted_prompt_is_reported_as_the_digest_of_its_text() {
        use flow_native::DigestsPrompt;
        let body = "#msg [\"psyche core Secondary\" \"go\"]";
        let digest = body.prompt_sha256();
        for prompt in [
            body.to_owned(),
            format!("\n\n<pasted_content id=\"04ee\">\n{body}\n</pasted_content id=\"04ee\">\n"),
            format!("<pasted_content id=\"00df\">\n{body}\n</pasted_content id=\"00df\">"),
        ] {
            assert_eq!(
                hook(
                    serde_json::json!({"hook_event_name": "UserPromptSubmit",
                        "session_id": "s", "prompt_id": "p", "prompt": prompt}),
                    Some("5a4d0b")
                )
                .call(),
                report(format!(
                    "Report.{{ «5a4d0b» None PromptSubmitted.{{ Some.«s» Some.«p» {digest} }} }}"
                ))
            );
        }
        assert!(matches!(
            hook(
                serde_json::json!({"hook_event_name": "UserPromptSubmit", "session_id": "s"}),
                Some("5a4d0b")
            )
            .call(),
            HookCall::Nothing(_)
        ));
    }

    #[test]
    fn without_a_claimed_flow_or_a_reported_event_nothing_is_sent() {
        assert!(matches!(
            hook(serde_json::json!({"hook_event_name": "Stop"}), None).call(),
            HookCall::Nothing(_)
        ));
        assert!(matches!(
            hook(
                serde_json::json!({"hook_event_name": "Notification"}),
                Some("5a4d0b")
            )
            .call(),
            HookCall::Nothing(_)
        ));
    }

    /// The hook's datom is one the Flow CLI reads as the Report it means.
    #[test]
    fn the_correlated_datoms_read_as_signal_flow_reports() {
        let read = |input: serde_json::Value| {
            let HookCall::Report(query) = hook(input, Some("5a4d0b")).call() else {
                panic!("the event is reported")
            };
            *query
        };
        assert_eq!(
            read(
                serde_json::json!({"hook_event_name": "Stop", "session_id": "s",
                "transcript_path": "/t/a »b».jsonl", "background_tasks": []})
            ),
            signal_flow::Query::Report(signal_flow::Report_Data {
                flow_id: signal_flow::FlowId("5a4d0b".into()),
                process_option: None,
                event: signal_flow::Event::Stopped(signal_flow::NativeStop {
                    native_session_id_option: Some(signal_flow::NativeSessionId("s".into())),
                    transcript_path_option: Some(signal_flow::TranscriptPath(
                        "/t/a »b».jsonl".into()
                    )),
                    last_assistant_message_option: signal_flow::LastAssistantMessageOption(None),
                    background_work: signal_flow::BackgroundWork(vec![]),
                }),
            })
        );
        assert_eq!(
            read(serde_json::json!({"hook_event_name": "UserPromptSubmit",
                "session_id": "s", "prompt": "abc"})),
            signal_flow::Query::Report(signal_flow::Report_Data {
                flow_id: signal_flow::FlowId("5a4d0b".into()),
                process_option: None,
                event: signal_flow::Event::PromptSubmitted(signal_flow::NativePrompt {
                    native_session_id_option: Some(signal_flow::NativeSessionId("s".into())),
                    prompt_id_option: None,
                    prompt_sha256: signal_flow::PromptSha256(
                        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into()
                    ),
                }),
            })
        );
        assert_eq!(
            read(serde_json::json!({"hook_event_name": "SessionEnd",
                "session_id": "s1", "reason": "clear"})),
            signal_flow::Query::Report(signal_flow::Report_Data {
                flow_id: signal_flow::FlowId("5a4d0b".into()),
                process_option: None,
                event: signal_flow::Event::Cleared(Some(signal_flow::NativeSessionId("s1".into()))),
            })
        );
        assert_eq!(
            read(serde_json::json!({"hook_event_name": "SessionStart", "session_id": "s2"})),
            signal_flow::Query::Report(signal_flow::Report_Data {
                flow_id: signal_flow::FlowId("5a4d0b".into()),
                process_option: None,
                event: signal_flow::Event::Started(Some(signal_flow::NativeSessionId("s2".into()))),
            })
        );
    }

    #[test]
    fn the_hook_datom_reads_as_a_signal_flow_report() {
        let HookCall::Report(query) = hook(
            serde_json::json!({"hook_event_name": "PostToolUse", "tool_name": "Bash"}),
            Some("5a4d0b"),
        )
        .call() else {
            panic!("a tool use is reported")
        };
        assert_eq!(
            *query,
            signal_flow::Query::Report(signal_flow::Report_Data {
                flow_id: signal_flow::FlowId("5a4d0b".into()),
                process_option: None,
                event: signal_flow::Event::ToolUsed("Bash".into()),
            })
        );
    }
}
