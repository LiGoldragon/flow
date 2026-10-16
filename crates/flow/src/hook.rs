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
//! Claude Code also runs it on every PreToolUse of the Agent tool, where it
//! guards the subagent roles Flow selects (`flow_nexus::role`). A call
//! naming one is denied unless it is made by the flow's main thread (no
//! `agent_id` in the event), sets neither `model` nor `effort`, and the
//! calling flow is one the role's caller policy names. The calling flow is
//! never read from `FLOW_ID` or the call: the `flow` CLI asks the Nexus
//! `ResolveCaller.None`, which the Nexus answers from the kernel-named
//! process at the other end of its socket and that process's Herdr pane,
//! and `Metaflows` then names the one Address awake as that flow. A denial
//! is Claude Code's PreToolUse decision on stdout:
//!
//!   {"hookSpecificOutput":{"hookEventName":"PreToolUse",
//!    "permissionDecision":"deny","permissionDecisionReason":"…"}}
//!
//! A permitted or ungoverned call gets no decision, and Claude Code's own
//! permission mode goes on.
//!
//! It never blocks the harness but by that decision: whatever happens it
//! exits 0, and it writes one tab-separated line on stderr (event, datom or
//! decision, the CLI's exit code, the CLI's whole output) for whoever reads
//! it.

use datom_codec::{Actualizing, Budget, Potential};
use flow_nexus::role::{
    AgentCall, AgentCaller, AgentDecision, AgentDenial, AuthorizesAgentCall, OpensSubagentRoles,
    ResolvesAgentCaller, SubagentRoles,
};
use protos::ReaderBudget;
use signal_flow::Response;
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
    Decide(AgentCall),
    Nothing(&'static str),
}

/// What one invocation writes: a PreToolUse decision for stdout, if any,
/// and the line for stderr.
#[derive(Debug, PartialEq, Eq)]
struct HookOutput {
    decision: Option<String>,
    log: String,
}

/// The calling flow as the Nexus resolves the `flow` CLI's own process.
struct FlowCliCaller {
    client: PathBuf,
}

/// Claude Code's PreToolUse decision for a denied Agent call.
trait RendersDenial {
    fn reason(&self) -> &'static str;
    fn decision(&self) -> String;
}

/// One reply of the Nexus to one inline Query datom, through the CLI.
trait AsksFlowNexus {
    fn ask(&self, query: &str) -> Option<Response>;
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
    /// The `flow` CLI installed beside this executable, else on PATH.
    fn client(&self) -> PathBuf;
    fn run(&self) -> HookOutput;
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
        if self.event_name() == "PreToolUse" {
            if self
                .input
                .get("tool_name")
                .and_then(serde_json::Value::as_str)
                != Some("Agent")
            {
                return HookCall::Nothing("PreToolUse of another tool");
            }
            let tool_input = self.input.get("tool_input");
            let set = |field: &str| {
                tool_input
                    .and_then(|tool_input| tool_input.get(field))
                    .is_some_and(|value| !value.is_null())
            };
            return HookCall::Decide(AgentCall {
                subagent_type: tool_input
                    .and_then(|tool_input| tool_input.get("subagent_type"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                nested: self
                    .input
                    .get("agent_id")
                    .is_some_and(|agent_id| !agent_id.is_null()),
                requests_model: set("model"),
                requests_effort: set("effort"),
            });
        }
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

impl RendersDenial for AgentDenial {
    fn reason(&self) -> &'static str {
        match self {
            Self::NestedCaller => "Flow refuses this role from inside a subagent",
            Self::ModelOverridden => "Flow refuses a model override for this role",
            Self::EffortOverridden => "Flow refuses an effort override for this role",
            Self::CallerUnresolved => "Flow could not resolve the calling flow from its process",
            Self::PolicyUnavailable => "Flow could not read this role's verified policy",
            Self::CallerNotPermitted => "this role's caller policy does not name the calling flow",
        }
    }

    fn decision(&self) -> String {
        serde_json::json!({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "deny",
                "permissionDecisionReason": self.reason(),
            }
        })
        .to_string()
    }
}

impl AsksFlowNexus for FlowCliCaller {
    fn ask(&self, query: &str) -> Option<Response> {
        let output = Command::new(&self.client).arg(query).output().ok()?;
        let reply = String::from_utf8(output.stdout).ok()?;
        let mut budget = Budget {
            remaining: 16 * 1024 * 1024,
            reader: ReaderBudget {
                remaining: 16 * 1024 * 1024,
            },
            depth: 0,
            maximum_depth: 1024,
        };
        Potential::<Response>::from(reply.trim().to_owned())
            .actualize(&mut budget)
            .ok()
    }
}

/// The caller the Nexus resolves from the CLI's process, then the one
/// metaflow awake as that flow, of the caller's aspect. Anything else
/// (no caller, a mismatch, none or more than one such metaflow) is
/// unresolved.
impl ResolvesAgentCaller for FlowCliCaller {
    fn agent_caller(&self) -> AgentCaller {
        let Some(Response::CallerResolved(caller)) = self.ask("ResolveCaller.None") else {
            return AgentCaller::Unresolved;
        };
        let Some(Response::Listed(metaflows)) = self.ask("Metaflows") else {
            return AgentCaller::Unresolved;
        };
        let awake = flow_ethos::State::Awake(caller.flow_id.clone());
        let mut held = metaflows
            .into_iter()
            .filter(|metaflow| metaflow.state == awake);
        match (held.next(), held.next()) {
            (Some(metaflow), None) if metaflow.address.aspect == caller.aspect => {
                AgentCaller::Resolved(metaflow.address)
            }
            _ => AgentCaller::Unresolved,
        }
    }
}

impl ReportsThroughCli for HarnessHook {
    fn client(&self) -> PathBuf {
        env::current_exe()
            .ok()
            .and_then(|path| path.parent().map(|directory| directory.join("flow")))
            .filter(|path| path.is_file())
            .unwrap_or_else(|| PathBuf::from("flow"))
    }

    fn run(&self) -> HookOutput {
        let datom = match self.call() {
            HookCall::Report(datom) => datom,
            HookCall::Decide(call) => {
                let decision = SubagentRoles::from_environment().decide(
                    &call,
                    FlowCliCaller {
                        client: self.client(),
                    },
                );
                return HookOutput {
                    decision: match &decision {
                        AgentDecision::Denied(denial) => Some(denial.decision()),
                        AgentDecision::Ungoverned | AgentDecision::Permitted => None,
                    },
                    log: format!(
                        "{}\t{}\t{decision:?}\t-",
                        self.event_name(),
                        call.subagent_type
                    ),
                };
            }
            HookCall::Nothing(reason) => {
                return HookOutput {
                    decision: None,
                    log: format!("{}\t-\t-\t{reason}", self.event_name()),
                };
            }
        };
        let log = match Command::new(self.client()).arg(&datom).output() {
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
        };
        HookOutput {
            decision: None,
            log,
        }
    }
}

fn main() -> ExitCode {
    let output = HarnessHook::from_environment().run();
    if let Some(decision) = output.decision {
        println!("{decision}");
    }
    eprintln!("{}", output.log);
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::{ChoosesHookCall, HarnessHook, HookCall, RendersDenial};
    use flow_nexus::role::{AgentCall, AgentDenial};

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

    /// An Agent call is read from the event alone: the role it names,
    /// whether a subagent made it, and whether it sets a model or an
    /// effort. FLOW_ID plays no part.
    #[test]
    fn an_agent_pre_tool_use_is_read_for_its_role_caller_thread_and_overrides() {
        assert_eq!(
            hook(
                serde_json::json!({
                    "hook_event_name": "PreToolUse",
                    "tool_name": "Agent",
                    "tool_input": {"subagent_type": "comment-relay", "prompt": "p", "description": "d"}
                }),
                None
            )
            .call(),
            HookCall::Decide(AgentCall {
                subagent_type: "comment-relay".into(),
                nested: false,
                requests_model: false,
                requests_effort: false,
            })
        );
        assert_eq!(
            hook(
                serde_json::json!({
                    "hook_event_name": "PreToolUse",
                    "tool_name": "Agent",
                    "agent_id": "a1b2",
                    "agent_type": "general-purpose",
                    "tool_input": {"subagent_type": "comment-relay", "model": "opus", "effort": "high"}
                }),
                Some("5a4d0b")
            )
            .call(),
            HookCall::Decide(AgentCall {
                subagent_type: "comment-relay".into(),
                nested: true,
                requests_model: true,
                requests_effort: true,
            })
        );
        assert!(matches!(
            hook(
                serde_json::json!({"hook_event_name": "PreToolUse", "tool_name": "Bash", "tool_input": {}}),
                Some("5a4d0b")
            )
            .call(),
            HookCall::Nothing(_)
        ));
    }

    /// The denial is Claude Code 2.1.294's PreToolUse hook output:
    /// hookSpecificOutput with hookEventName, permissionDecision and
    /// permissionDecisionReason.
    #[test]
    fn a_denial_is_the_native_pre_tool_use_deny_decision() {
        assert_eq!(
            AgentDenial::CallerNotPermitted.decision(),
            concat!(
                r#"{"hookSpecificOutput":{"hookEventName":"PreToolUse","#,
                r#""permissionDecision":"deny","#,
                r#""permissionDecisionReason":"this role's caller policy does not name the calling flow"}}"#
            )
        );
    }
}
