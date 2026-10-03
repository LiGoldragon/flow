//! `flow-hook`: the Claude Code command hook Flow writes into every Claude
//! flow it launches. Claude Code runs it on SessionStart, PostToolUse and
//! Stop with the event as JSON on stdin; it turns the event into one
//! `Report.{ FlowId Event }` datom and hands it to the `flow` CLI beside it,
//! which is the only thing here that speaks Signal.
//!
//!   SessionStart -> Report.{ «FLOW_ID» Started }
//!   PostToolUse  -> Report.{ «FLOW_ID» ToolUsed.«tool_name» }
//!   Stop         -> Report.{ «FLOW_ID» Stopped }
//!
//! The FlowId is the `FLOW_ID` the flow claimed, read from the harness's
//! environment, never derived from the harness's session id (ruling 12 of
//! flow f1c841). With no FLOW_ID, or on any other event, it reports
//! nothing.
//!
//! It never blocks the harness: whatever happens it exits 0, and it writes
//! one tab-separated line on stderr (event, datom, the CLI's exit code, the
//! CLI's whole output) for whoever reads it.

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

trait ReportsThroughCli {
    fn run(&self) -> String;
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
            "PostToolUse" => match self
                .input
                .get("tool_name")
                .and_then(serde_json::Value::as_str)
            {
                Some(tool_name) => format!("ToolUsed.{}", tool_name.quoted()),
                None => return HookCall::Nothing("PostToolUse without a tool_name"),
            },
            "Stop" => "Stopped".to_owned(),
            _ => return HookCall::Nothing("no Report for this event"),
        };
        HookCall::Report(format!("Report.{{ {} {event} }}", flow_id.quoted()))
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
    eprintln!("{}", HarnessHook::from_environment().run());
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::{ChoosesHookCall, HarnessHook, HookCall};

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
                serde_json::json!({"hook_event_name": "Stop", "prompt_id": "p"}),
                Some("5a4d0b")
            )
            .call(),
            HookCall::Report("Report.{ «5a4d0b» Stopped }".into())
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
