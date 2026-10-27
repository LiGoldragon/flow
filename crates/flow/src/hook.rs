//! `flow-hook`: the Claude Code command hook Flow writes into every Claude
//! flow it launches. Claude Code runs it on SessionStart, PostToolUse,
//! UserPromptSubmit, Notification, Stop and SessionEnd with the event as JSON on stdin; it turns the
//! event into one `Report.{ Reporter Event }` datom and hands it to the `flow`
//! CLI beside it, which is the only thing here that speaks Signal.
//!
//!   SessionStart     -> Report.{ Flow.«FLOW_ID» Started.Some.«session_id» }
//!   PostToolUse      -> Report.{ Flow.«FLOW_ID» ToolUsed.«tool_name» }
//!   UserPromptSubmit -> Report.{ Flow.«FLOW_ID» PromptSubmitted.{ Some.«session_id» Some.«prompt_id» SHA256 } }
//!   Notification     -> Report.{ Flow.«FLOW_ID» Notified.«message» }
//!   Stop             -> Report.{ Flow.«FLOW_ID» Stopped.{ Some.«session_id» Some.«transcript_path» Some.«last_assistant_message» } }
//!   SessionEnd       -> Report.{ Flow.«FLOW_ID» Ended.Some.«session_id» }
//!   SessionEnd clear -> Report.{ Flow.«FLOW_ID» Cleared.Some.«session_id» }
//!
//! A SessionEnd whose reason is `clear` is a session change, not an exit:
//! the harness goes on in the session the SessionStart after it names.
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
//! It never blocks the harness: whatever happens it exits 0, writes nothing
//! on stdout (which UserPromptSubmit would add to the model's context), and
//! writes one tab-separated line on stderr (event, datom, the CLI's exit
//! code, the CLI's whole output) for whoever reads it.

use flow_native::{DigestsPrompt, ReadsClaudePaste};
use std::{
    env,
    io::Read,
    path::PathBuf,
    process::{Command, ExitCode},
};

/// One hook invocation: the event Claude Code wrote, and the flow it ran in.
struct HarnessHook {
    input: serde_json::Value,
    flow_id: Option<String>,
    herdr_executable: Option<PathBuf>,
    herdr_session: Option<String>,
    herdr_pane_id: Option<String>,
    readiness_socket: Option<PathBuf>,
}

/// What the hook does with one event.
#[derive(Debug, PartialEq, Eq)]
enum HookCall {
    Report(String),
    Nothing(&'static str),
}

trait ReadsHookInput: Sized {
    fn from_environment() -> Self;
}

trait ChoosesHookCall {
    fn event_name(&self) -> String;
    fn call(&self) -> HookCall;
}

trait QuotesDatomString {
    /// A datom string in guillemets: every glyph is content until the
    /// closing guillemet, which is escaped with a backslash.
    fn quoted(&self) -> String;
}

trait ReadsHookField {
    /// The input's string field as a datom Option: `Some.«value»`, or
    /// `None` when the input does not carry it.
    fn optional(&self, field: &str) -> String;
}

trait ReportsThroughCli {
    fn run(&self) -> String;
}

/// Reports the lifecycle Flow owns for a Claude process it started in a
/// Herdr pane. Herdr remains the pane owner; this hook is the native
/// handshake which makes readiness and later prompt transitions explicit.
trait ReportsOwnedClaudeLifecycle {
    fn lifecycle_run(&self) -> String;
}

impl ReadsHookInput for HarnessHook {
    fn from_environment() -> Self {
        let mut text = String::new();
        let _ = std::io::stdin().read_to_string(&mut text);
        Self {
            input: serde_json::from_str(&text).unwrap_or(serde_json::Value::Null),
            flow_id: env::var("FLOW_ID")
                .ok()
                .filter(|flow_id| !flow_id.is_empty()),
            herdr_executable: env::var_os("FLOW_HERDR_EXECUTABLE").map(PathBuf::from),
            herdr_session: env::var("FLOW_HERDR_SESSION")
                .ok()
                .filter(|session| !session.is_empty()),
            herdr_pane_id: env::var("HERDR_PANE_ID")
                .ok()
                .filter(|pane| !pane.is_empty()),
            readiness_socket: env::var_os("FLOW_CLAUDE_READY_SOCKET").map(PathBuf::from),
        }
    }
}

impl ReportsOwnedClaudeLifecycle for HarnessHook {
    fn lifecycle_run(&self) -> String {
        let (Some(herdr), Some(session), Some(pane)) = (
            &self.herdr_executable,
            &self.herdr_session,
            &self.herdr_pane_id,
        ) else {
            return "lifecycle-unconfigured".into();
        };
        let state = match self.event_name().as_str() {
            "SessionStart" | "Stop" => "idle",
            "UserPromptSubmit" | "PostToolUse" => "working",
            "SessionEnd" => "unknown",
            _ => return "lifecycle-unchanged".into(),
        };
        let mut command = Command::new(herdr);
        command.args([
            "--session",
            session,
            "pane",
            "report-agent",
            pane,
            "--source",
            "flow",
            "--agent",
            "claude",
            "--state",
            state,
        ]);
        if let Some(session) = self
            .input
            .get("session_id")
            .and_then(serde_json::Value::as_str)
            .filter(|session| !session.is_empty())
        {
            command.args(["--agent-session-id", session]);
        }
        match command.output() {
            Ok(output) if output.status.success() => {
                if self.event_name() == "SessionStart"
                    && let Some(socket) = &self.readiness_socket
                    && let Ok(sender) = std::os::unix::net::UnixDatagram::unbound()
                {
                    // This endpoint carries only an event edge. The typed
                    // Herdr report above and the actual argv carry and
                    // validate the session identity.
                    let _ = sender.send_to(&[], socket);
                }
                format!("lifecycle-{state}")
            }
            Ok(output) => format!(
                "lifecycle-refused:{}",
                String::from_utf8_lossy(&output.stderr)
                    .trim_end()
                    .replace('\n', " ")
            ),
            Err(error) => format!("lifecycle-unattempted:{error}"),
        }
    }
}

impl QuotesDatomString for str {
    fn quoted(&self) -> String {
        format!("«{}»", self.replace('»', "\\»"))
    }
}

impl ReadsHookField for HarnessHook {
    fn optional(&self, field: &str) -> String {
        match self.input.get(field).and_then(serde_json::Value::as_str) {
            Some(value) => format!("Some.{}", value.quoted()),
            None => "None".to_owned(),
        }
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
        let reporter = match &self.flow_id {
            Some(flow_id) => format!("Flow.{}", flow_id.quoted()),
            None => match self
                .input
                .get("session_id")
                .and_then(serde_json::Value::as_str)
            {
                Some(session_id) if !session_id.is_empty() => {
                    format!("Session.{}", session_id.quoted())
                }
                _ => return HookCall::Nothing("no flow or session identity in the harness event"),
            },
        };
        let event = match self.event_name().as_str() {
            "SessionStart" => format!("Started.{}", self.optional("session_id")),
            "PostToolUse" => match self
                .input
                .get("tool_name")
                .and_then(serde_json::Value::as_str)
            {
                Some(tool_name) => format!("ToolUsed.{}", tool_name.quoted()),
                None => return HookCall::Nothing("PostToolUse without a tool_name"),
            },
            "UserPromptSubmit" => {
                match self.input.get("prompt").and_then(serde_json::Value::as_str) {
                    Some(prompt) => format!(
                        "PromptSubmitted.{{ {} {} {} }}",
                        self.optional("session_id"),
                        self.optional("prompt_id"),
                        prompt.submitted_claude_text().prompt_sha256()
                    ),
                    None => return HookCall::Nothing("UserPromptSubmit without a prompt"),
                }
            }
            "Notification" => match self
                .input
                .get("message")
                .and_then(serde_json::Value::as_str)
            {
                Some(message) => format!("Notified.{}", message.quoted()),
                None => return HookCall::Nothing("Notification without a message"),
            },
            // A session /clear ended: the harness goes on in a new
            // session, so the flow has not exited.
            "SessionEnd"
                if self.input.get("reason").and_then(serde_json::Value::as_str)
                    == Some("clear") =>
            {
                format!("Cleared.{}", self.optional("session_id"))
            }
            // The session ends: the flow has exited, whether or not
            // anything looks at its pane.
            "SessionEnd" => format!("Ended.{}", self.optional("session_id")),
            "Stop" => format!(
                "Stopped.{{ {} {} {} }}",
                self.optional("session_id"),
                self.optional("transcript_path"),
                self.optional("last_assistant_message")
            ),
            _ => return HookCall::Nothing("no Report for this event"),
        };
        HookCall::Report(format!("Report.{{ {reporter} {event} }}"))
    }
}

impl ReportsThroughCli for HarnessHook {
    fn run(&self) -> String {
        let datom = match self.call() {
            HookCall::Report(datom) => datom,
            HookCall::Nothing(reason) => return format!("{}\t-\t-\t{reason}", self.event_name()),
        };
        // The `flow` CLI installed beside this executable, else on PATH.
        let client = env::current_exe()
            .ok()
            .and_then(|path| path.parent().map(|directory| directory.join("flow")))
            .filter(|path| path.is_file())
            .unwrap_or_else(|| PathBuf::from("flow"));
        match Command::new(client).arg(&datom).output() {
            Ok(output) => {
                let reply = format!(
                    "{}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                format!(
                    "{}\t{datom}\t{}\t{}",
                    self.event_name(),
                    output.status.code().unwrap_or(-1),
                    reply.trim_end().replace('\n', " ")
                )
            }
            Err(error) => format!("{}\t{datom}\t-\t{error}", self.event_name()),
        }
    }
}

fn main() -> ExitCode {
    let hook = HarnessHook::from_environment();
    let lifecycle = hook.lifecycle_run();
    let report = hook.run();
    if matches!(
        lifecycle.as_str(),
        "lifecycle-unconfigured" | "lifecycle-unchanged"
    ) {
        eprintln!("{report}");
    } else {
        eprintln!("{lifecycle}\t{report}");
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::{ChoosesHookCall, HarnessHook, HookCall, ReportsOwnedClaudeLifecycle};

    fn hook(input: serde_json::Value, flow_id: Option<&str>) -> HarnessHook {
        HarnessHook {
            input,
            flow_id: flow_id.map(str::to_owned),
            herdr_executable: None,
            herdr_session: None,
            herdr_pane_id: None,
            readiness_socket: None,
        }
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
            HookCall::Report("Report.{ Flow.«5a4d0b» Started.Some.«s» }".into())
        );
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "PostToolUse", "tool_name": "Bash"}),
                Some("5a4d0b")
            )
            .call(),
            HookCall::Report("Report.{ Flow.«5a4d0b» ToolUsed.«Bash» }".into())
        );
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "Stop", "prompt_id": "p",
                    "session_id": "s", "transcript_path": "/t/s.jsonl",
                    "last_assistant_message": "answer"}),
                Some("5a4d0b")
            )
            .call(),
            HookCall::Report(
                "Report.{ Flow.«5a4d0b» Stopped.{ Some.«s» Some.«/t/s.jsonl» Some.«answer» } }"
                    .into()
            )
        );
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "Stop"}),
                Some("5a4d0b")
            )
            .call(),
            HookCall::Report("Report.{ Flow.«5a4d0b» Stopped.{ None None None } }".into())
        );
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "Stop",
                    "last_assistant_message": ""}),
                Some("5a4d0b")
            )
            .call(),
            HookCall::Report("Report.{ Flow.«5a4d0b» Stopped.{ None None Some.«» } }".into())
        );
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "SessionEnd", "session_id": "s",
                    "reason": "prompt_input_exit"}),
                Some("5a4d0b")
            )
            .call(),
            HookCall::Report("Report.{ Flow.«5a4d0b» Ended.Some.«s» }".into())
        );
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "SessionEnd", "session_id": "s",
                    "reason": "logout"}),
                Some("5a4d0b")
            )
            .call(),
            HookCall::Report("Report.{ Flow.«5a4d0b» Ended.Some.«s» }".into())
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
            HookCall::Report("Report.{ Flow.«5a4d0b» Cleared.Some.«s1» }".into())
        );
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "SessionStart", "session_id": "s2",
                    "source": "clear"}),
                Some("5a4d0b")
            )
            .call(),
            HookCall::Report("Report.{ Flow.«5a4d0b» Started.Some.«s2» }".into())
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
                HookCall::Report(format!(
                    "Report.{{ Flow.«5a4d0b» PromptSubmitted.{{ Some.«s» Some.«p» {digest} }} }}"
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
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "Notification", "session_id": "s", "message": "ready"}),
                None
            ).call(),
            HookCall::Report("Report.{ Session.«s» Notified.«ready» }".into())
        );
        assert!(matches!(
            hook(
                serde_json::json!({"hook_event_name": "Notification", "session_id": "s"}),
                None
            )
            .call(),
            HookCall::Nothing(_)
        ));
    }

    /// Flow's owned Claude hook reports the exact pane, lifecycle source,
    /// harness, state and native session through the configured Herdr
    /// executable. This is the readiness handshake for the process Flow
    /// started, rather than Herdr's automatic executable detection.
    #[test]
    fn a_flow_owned_claude_session_reports_its_readiness_identity() {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = tempfile::tempdir().unwrap();
        let log = directory.path().join("arguments");
        let executable = directory.path().join("herdr");
        std::fs::write(
            &executable,
            format!("#!/bin/sh\nprintf '%s\\n' \"$*\" > '{}'\n", log.display()),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&executable).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&executable, permissions).unwrap();
        let hook = HarnessHook {
            input: serde_json::json!({
                "hook_event_name": "SessionStart",
                "session_id": "native-session"
            }),
            flow_id: Some("5a4d0b".into()),
            herdr_executable: Some(executable),
            herdr_session: Some("flow-session".into()),
            herdr_pane_id: Some("w7:p1".into()),
            readiness_socket: None,
        };

        assert_eq!(hook.lifecycle_run(), "lifecycle-idle");
        assert_eq!(
            std::fs::read_to_string(log).unwrap().trim_end(),
            "--session flow-session pane report-agent w7:p1 --source flow --agent claude --state idle --agent-session-id native-session"
        );
    }

    /// The hook's datom is one the Flow CLI reads as the Report it means.
    #[test]
    fn the_correlated_datoms_read_as_signal_flow_reports() {
        use datom_codec::{Actualizing, Budget, Potential};
        use protos::ReaderBudget;
        let read = |input: serde_json::Value| {
            let HookCall::Report(datom) = hook(input, Some("5a4d0b")).call() else {
                panic!("the event is reported")
            };
            let mut budget = Budget {
                remaining: 4096,
                reader: ReaderBudget { remaining: 4096 },
                depth: 0,
                maximum_depth: 1024,
            };
            Potential::<signal_flow::Query>::from(datom)
                .actualize(&mut budget)
                .unwrap()
        };
        assert_eq!(
            read(
                serde_json::json!({"hook_event_name": "Stop", "session_id": "s",
                "transcript_path": "/t/a »b».jsonl"})
            ),
            signal_flow::Query::Report(signal_flow::Report_Data {
                reporter: signal_flow::Reporter::Flow("5a4d0b".into()),
                event: signal_flow::Event::Stopped(signal_flow::NativeStop {
                    native_session_id_option: Some("s".into()),
                    transcript_path_option: Some("/t/a »b».jsonl".into()),
                    last_assistant_message_option: None,
                }),
            })
        );
        assert_eq!(
            read(serde_json::json!({"hook_event_name": "UserPromptSubmit",
                "session_id": "s", "prompt": "abc"})),
            signal_flow::Query::Report(signal_flow::Report_Data {
                reporter: signal_flow::Reporter::Flow("5a4d0b".into()),
                event: signal_flow::Event::PromptSubmitted(signal_flow::NativePrompt {
                    native_session_id_option: Some("s".into()),
                    prompt_id_option: None,
                    prompt_sha256:
                        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
                }),
            })
        );
        assert_eq!(
            read(serde_json::json!({"hook_event_name": "SessionEnd",
                "session_id": "s1", "reason": "clear"})),
            signal_flow::Query::Report(signal_flow::Report_Data {
                reporter: signal_flow::Reporter::Flow("5a4d0b".into()),
                event: signal_flow::Event::Cleared(Some("s1".into())),
            })
        );
        assert_eq!(
            read(serde_json::json!({"hook_event_name": "SessionStart", "session_id": "s2"})),
            signal_flow::Query::Report(signal_flow::Report_Data {
                reporter: signal_flow::Reporter::Flow("5a4d0b".into()),
                event: signal_flow::Event::Started(Some("s2".into())),
            })
        );
    }

    #[test]
    fn the_hook_datom_reads_as_a_signal_flow_report() {
        use datom_codec::{Actualizing, Budget, Potential};
        use protos::ReaderBudget;
        let HookCall::Report(datom) = hook(
            serde_json::json!({"hook_event_name": "PostToolUse", "tool_name": "Bash"}),
            Some("5a4d0b"),
        )
        .call() else {
            panic!("a tool use is reported")
        };
        let mut budget = Budget {
            remaining: 4096,
            reader: ReaderBudget { remaining: 4096 },
            depth: 0,
            maximum_depth: 1024,
        };
        assert_eq!(
            Potential::<signal_flow::Query>::from(datom)
                .actualize(&mut budget)
                .unwrap(),
            signal_flow::Query::Report(signal_flow::Report_Data {
                reporter: signal_flow::Reporter::Flow("5a4d0b".into()),
                event: signal_flow::Event::ToolUsed("Bash".into()),
            })
        );
    }
}
