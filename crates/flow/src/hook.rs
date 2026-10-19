//! `flow-hook`: the Claude Code command hook Flow writes into every Claude
//! flow it launches. Claude Code runs it on SessionStart, PostToolUse,
//! UserPromptSubmit and Stop with the event as JSON on stdin; it turns the
//! event into one `Report.{ FlowId Event }` datom and hands it to the `flow`
//! CLI beside it, which is the only thing here that speaks Signal.
//!
//!   SessionStart     -> Report.{ «FLOW_ID» Started }
//!   PostToolUse      -> Report.{ «FLOW_ID» ToolUsed.«tool_name» }
//!   PostToolUse of Artifact answering a url
//!                    -> Report.{ «FLOW_ID» Published.{ Some.«session_id» Some.«prompt_id» «url» } }
//!   UserPromptSubmit -> Report.{ «FLOW_ID» PromptSubmitted.{ Some.«session_id» Some.«prompt_id» SHA256 Origin } }
//!   Stop             -> Report.{ «FLOW_ID» Stopped.{ Some.«session_id» Some.«transcript_path» } }
//!
//! A Stop and a submitted prompt carry the native identity Claude Code gave
//! the hook, so the Nexus acts on them only when they are the flow's own
//! registered session; a field the input does not carry is `None`. A
//! submitted prompt travels as the SHA-256 of the text Claude Code took
//! (its paste wrapper taken off), never as the text: one argument holds at
//! most 128 KiB on Linux, and the digest is an exact match of the whole
//! prompt. Its origin is read from that text: `#psyche` and `#psyches` are
//! the living's words relayed (Psyche), `#msg` a flow's message (Message),
//! Flow's queued-work opening a hand-over (Queued), Claude Code's
//! `<task-notification>` a background task's end (Notification), and
//! anything else was typed (Typed).
//!
//! The Artifact tool answers a publication with the book's `url`; any other
//! Artifact answer (a read, a list, comments, an error) carries none and is
//! an ordinary tool use.
//!
//! A Stop is the one event whose answer the hook acts on. When Flow answers
//! `TurnHeld`, the flow owes the living a book: the hook writes Claude
//! Code's block decision on stdout, `{"decision":"block","reason":...}`,
//! and the turn goes on with that reason. When Flow cannot be reached, or
//! does not answer, the hook blocks with "Flow unreachable: make the book,
//! then end". Any other answer lets the turn end.
//!
//! The FlowId is the `FLOW_ID` the flow claimed, read from the harness's
//! environment, never derived from the harness's session id (ruling 12 of
//! flow f1c841). With no FLOW_ID, or on any other event, it reports
//! nothing and blocks nothing.
//!
//! It always exits 0. It writes nothing else on stdout (which
//! UserPromptSubmit would add to the model's context), and writes one
//! tab-separated line on stderr (event, datom, the CLI's exit code, the
//! CLI's whole output) for whoever reads it.

use flow_native::{DigestsPrompt, QUEUED_WORK_OPENING, ReadsClaudePaste};
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
}

/// What the hook does with one event.
#[derive(Debug, PartialEq, Eq)]
enum HookCall {
    Report(String),
    Nothing(&'static str),
}

/// What one hook run leaves: the line for stderr, and Claude Code's block
/// decision for stdout when it holds a Stop.
#[derive(Debug, PartialEq, Eq)]
struct HookRun {
    log: String,
    decision: Option<String>,
}

/// What the `flow` CLI gave back for one Report: its exit code, when it
/// ran and ended, and what it printed on stdout.
#[derive(Debug, PartialEq, Eq)]
struct CliAnswer {
    exit: Option<i32>,
    stdout: String,
}

trait ReadsHookInput: Sized {
    fn from_environment() -> Self;
}

trait ChoosesHookCall {
    fn event_name(&self) -> String;
    fn call(&self) -> HookCall;
    /// The datom variant of `PromptOrigin` for the text Claude Code took.
    fn origin_of(&self, text: &str) -> &'static str;
    /// The book an Artifact tool use published: its URL, when the tool
    /// answered one.
    fn published_url(&self) -> Option<String>;
    /// Claude Code's block decision for a Stop, from Flow's answer; None
    /// lets the turn end, and so does any other event.
    fn decision(&self, answer: &CliAnswer) -> Option<String>;
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
    fn run(&self) -> HookRun;
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
        let Some(flow_id) = &self.flow_id else {
            return HookCall::Nothing("no FLOW_ID in the harness environment");
        };
        let event = match self.event_name().as_str() {
            "SessionStart" => "Started".to_owned(),
            "PostToolUse" => match (
                self.input
                    .get("tool_name")
                    .and_then(serde_json::Value::as_str),
                self.published_url(),
            ) {
                (Some(_), Some(url)) => format!(
                    "Published.{{ {} {} {} }}",
                    self.optional("session_id"),
                    self.optional("prompt_id"),
                    url.quoted()
                ),
                (Some(tool_name), None) => format!("ToolUsed.{}", tool_name.quoted()),
                (None, _) => return HookCall::Nothing("PostToolUse without a tool_name"),
            },
            "UserPromptSubmit" => {
                match self.input.get("prompt").and_then(serde_json::Value::as_str) {
                    Some(prompt) => {
                        let text = prompt.submitted_claude_text();
                        format!(
                            "PromptSubmitted.{{ {} {} {} {} }}",
                            self.optional("session_id"),
                            self.optional("prompt_id"),
                            text.prompt_sha256(),
                            self.origin_of(text)
                        )
                    }
                    None => return HookCall::Nothing("UserPromptSubmit without a prompt"),
                }
            }
            "Stop" => format!(
                "Stopped.{{ {} {} }}",
                self.optional("session_id"),
                self.optional("transcript_path")
            ),
            _ => return HookCall::Nothing("no Report for this event"),
        };
        HookCall::Report(format!("Report.{{ {} {event} }}", flow_id.quoted()))
    }

    fn origin_of(&self, text: &str) -> &'static str {
        let text = text.trim_start();
        let head = text
            .split(|glyph: char| glyph.is_whitespace() || glyph == '[')
            .next()
            .unwrap_or_default();
        match head {
            "#psyche" | "#psyches" => "Psyche",
            "#msg" => "Message",
            _ if text.starts_with(QUEUED_WORK_OPENING) => "Queued",
            _ if text.starts_with("<task-notification>") => "Notification",
            _ => "Typed",
        }
    }

    fn published_url(&self) -> Option<String> {
        (self
            .input
            .get("tool_name")
            .and_then(serde_json::Value::as_str)
            == Some("Artifact"))
        .then(|| {
            self.input
                .pointer("/tool_response/url")
                .and_then(serde_json::Value::as_str)
                .filter(|url| !url.is_empty())
                .map(str::to_owned)
        })
        .flatten()
    }

    fn decision(&self, answer: &CliAnswer) -> Option<String> {
        use datom_codec::{Actualizing, Budget, Potential};
        use protos::ReaderBudget;
        if self.flow_id.is_none() || self.event_name() != "Stop" {
            return None;
        }
        let mut budget = Budget {
            remaining: 65_536,
            reader: ReaderBudget { remaining: 65_536 },
            depth: 0,
            maximum_depth: 1_024,
        };
        let reply = (answer.exit == Some(0))
            .then(|| {
                Potential::<signal_flow::Response>::from(answer.stdout.trim().to_owned())
                    .actualize(&mut budget)
                    .ok()
            })
            .flatten();
        let reason = match reply {
            Some(signal_flow::Response::TurnHeld(reason)) => reason,
            Some(_) => return None,
            None => "Flow unreachable: make the book, then end".to_owned(),
        };
        Some(serde_json::json!({ "decision": "block", "reason": reason }).to_string())
    }
}

impl ReportsThroughCli for HarnessHook {
    fn run(&self) -> HookRun {
        let datom = match self.call() {
            HookCall::Report(datom) => datom,
            HookCall::Nothing(reason) => {
                return HookRun {
                    log: format!("{}\t-\t-\t{reason}", self.event_name()),
                    decision: None,
                };
            }
        };
        // The `flow` CLI installed beside this executable, else on PATH.
        let client = env::current_exe()
            .ok()
            .and_then(|path| path.parent().map(|directory| directory.join("flow")))
            .filter(|path| path.is_file())
            .unwrap_or_else(|| PathBuf::from("flow"));
        match Command::new(client).arg(&datom).output() {
            Ok(output) => {
                let answer = CliAnswer {
                    exit: output.status.code(),
                    stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
                };
                let reply = format!(
                    "{}{}",
                    answer.stdout,
                    String::from_utf8_lossy(&output.stderr)
                );
                HookRun {
                    log: format!(
                        "{}\t{datom}\t{}\t{}",
                        self.event_name(),
                        answer.exit.unwrap_or(-1),
                        reply.trim_end().replace('\n', " ")
                    ),
                    decision: self.decision(&answer),
                }
            }
            Err(error) => HookRun {
                log: format!("{}\t{datom}\t-\t{error}", self.event_name()),
                decision: self.decision(&CliAnswer {
                    exit: None,
                    stdout: String::new(),
                }),
            },
        }
    }
}

fn main() -> ExitCode {
    let run = HarnessHook::from_environment().run();
    eprintln!("{}", run.log);
    if let Some(decision) = run.decision {
        println!("{decision}");
    }
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::{ChoosesHookCall, CliAnswer, HarnessHook, HookCall};

    fn hook(input: serde_json::Value, flow_id: Option<&str>) -> HarnessHook {
        HarnessHook {
            input,
            flow_id: flow_id.map(str::to_owned),
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
            HookCall::Report("Report.{ «5a4d0b» Started }".into())
        );
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "PostToolUse", "tool_name": "Bash"}),
                Some("5a4d0b")
            )
            .call(),
            HookCall::Report("Report.{ «5a4d0b» ToolUsed.«Bash» }".into())
        );
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "Stop", "prompt_id": "p",
                    "session_id": "s", "transcript_path": "/t/s.jsonl"}),
                Some("5a4d0b")
            )
            .call(),
            HookCall::Report("Report.{ «5a4d0b» Stopped.{ Some.«s» Some.«/t/s.jsonl» } }".into())
        );
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "Stop"}),
                Some("5a4d0b")
            )
            .call(),
            HookCall::Report("Report.{ «5a4d0b» Stopped.{ None None } }".into())
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
                    "Report.{{ «5a4d0b» PromptSubmitted.{{ Some.«s» Some.«p» {digest} Message }} }}"
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

    /// The origin is read from the text Claude Code took: the living's
    /// words relayed as psyche, a flow's message, Flow's hand-over of
    /// queued work, a background task's notice, or anything typed.
    #[test]
    fn a_submitted_prompt_carries_the_origin_its_text_shows() {
        let origin = |prompt: &str| {
            let HookCall::Report(datom) = hook(
                serde_json::json!({"hook_event_name": "UserPromptSubmit",
                    "session_id": "s", "prompt_id": "p", "prompt": prompt}),
                Some("5a4d0b"),
            )
            .call() else {
                panic!("a prompt is reported")
            };
            datom
                .trim_end_matches(" } }")
                .rsplit(' ')
                .next()
                .unwrap()
                .to_owned()
        };
        for (prompt, expected) in [
            (
                "#psyche [\"Psyche core Secondary\" \"ctx\" \"words\"]",
                "Psyche",
            ),
            ("#psyches [[\"ctx\" \"words\"]]", "Psyche"),
            ("  #psyche[\"x\"]", "Psyche"),
            ("#msg [\"Mind flow Secondary\" \"done\"]", "Message"),
            (
                "\n\n<pasted_content id=\"04ee\">\n#psyche [\"a\" \"b\"]\n</pasted_content id=\"04ee\">\n",
                "Psyche",
            ),
            (
                "Work queued for this metaflow while no flow of it was running, oldest first, each with who sent it:\n{ Unknown Order.go }",
                "Queued",
            ),
            (
                "<task-notification>\n<status>completed</status>\n</task-notification>",
                "Notification",
            ),
            ("make me a book about the store", "Typed"),
            ("#msgs are not messages", "Typed"),
            ("#psychedelic", "Typed"),
        ] {
            assert_eq!(origin(prompt), expected, "{prompt}");
        }
    }

    /// An Artifact tool use that answered a url published a book; a read,
    /// a list or an error of the same tool is an ordinary tool use.
    #[test]
    fn an_artifact_publication_is_reported_as_published() {
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "PostToolUse", "tool_name": "Artifact",
                    "session_id": "s", "prompt_id": "p", "agent_id": "a1",
                    "tool_input": {"file_path": "/s/artifact_1.html"},
                    "tool_response": {"url": "https://claude.ai/code/artifact/4fcc3326",
                        "artifact_id": "4fcc3326", "version": 1}}),
                Some("5a4d0b")
            )
            .call(),
            HookCall::Report(
                "Report.{ «5a4d0b» Published.{ Some.«s» Some.«p» «https://claude.ai/code/artifact/4fcc3326» } }"
                    .into()
            )
        );
        for response in [
            serde_json::json!({"artifactRead": {}, "read": {}}),
            serde_json::json!({"threads": []}),
            serde_json::json!("Error: the publish was refused"),
        ] {
            assert_eq!(
                hook(
                    serde_json::json!({"hook_event_name": "PostToolUse", "tool_name": "Artifact",
                        "session_id": "s", "tool_response": response}),
                    Some("5a4d0b")
                )
                .call(),
                HookCall::Report("Report.{ «5a4d0b» ToolUsed.«Artifact» }".into())
            );
        }
    }

    /// A Stop Flow holds is blocked with Flow's reason; a Stop Flow cannot
    /// answer is blocked as unreachable; any other answer, and any other
    /// event, lets the turn end.
    #[test]
    fn a_held_or_unanswered_stop_is_blocked() {
        let stop = hook(
            serde_json::json!({"hook_event_name": "Stop", "session_id": "s",
                "transcript_path": "/t/s.jsonl", "stop_hook_active": false}),
            Some("5a4d0b"),
        );
        let answer = |exit, stdout: &str| CliAnswer {
            exit,
            stdout: stdout.to_owned(),
        };
        let block = |reason: &str| {
            Some(serde_json::json!({"decision": "block", "reason": reason}).to_string())
        };
        assert_eq!(
            stop.decision(&answer(
                Some(0),
                "TurnHeld.«Make the book for the living's words before ending: the words typed»\n"
            )),
            block("Make the book for the living's words before ending: the words typed")
        );
        for unanswered in [
            answer(Some(2), ""),
            answer(None, ""),
            answer(Some(0), "not a reply\n"),
        ] {
            assert_eq!(
                stop.decision(&unanswered),
                block("Flow unreachable: make the book, then end")
            );
        }
        assert_eq!(stop.decision(&answer(Some(0), "Reported\n")), None);
        assert_eq!(
            stop.decision(&answer(Some(0), "Refused.Unknown.FlowId.5a4d0b\n")),
            None
        );
        assert_eq!(
            hook(
                serde_json::json!({"hook_event_name": "UserPromptSubmit", "prompt": "x"}),
                Some("5a4d0b")
            )
            .decision(&answer(Some(2), "")),
            None
        );
        assert_eq!(
            hook(serde_json::json!({"hook_event_name": "Stop"}), None).decision(&answer(None, "")),
            None
        );
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
                flow_id: "5a4d0b".into(),
                event: signal_flow::Event::Stopped(signal_flow::NativeStop {
                    native_session_id_option: Some("s".into()),
                    transcript_path_option: Some("/t/a »b».jsonl".into()),
                }),
            })
        );
        assert_eq!(
            read(serde_json::json!({"hook_event_name": "UserPromptSubmit",
                "session_id": "s", "prompt": "abc"})),
            signal_flow::Query::Report(signal_flow::Report_Data {
                flow_id: "5a4d0b".into(),
                event: signal_flow::Event::PromptSubmitted(signal_flow::NativePrompt {
                    native_session_id_option: Some("s".into()),
                    prompt_id_option: None,
                    prompt_sha256:
                        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad".into(),
                    prompt_origin: signal_flow::PromptOrigin::Typed,
                }),
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
                flow_id: "5a4d0b".into(),
                event: signal_flow::Event::ToolUsed("Bash".into()),
            })
        );
    }
}
