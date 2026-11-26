//! What a harness reports, typed into one Event and recorded as a
//! transition of the flow it is of (`ethos/transitions.ethos`).
//!
//! A Claude flow reports through its hooks (`reporting`): its fresh session
//! Started, its own prompt and each tool boundary Working, its own Stop
//! Idle with the size its transcript shows, its own session's end Ended. A
//! Started naming a session the flow already started is the session going
//! on, after a restart's resume or past a compaction: its rest when the
//! transcript shows its main turn ended, else Working. A Codex flow reports
//! through its thread's turn ledger (`codex_turns`) and its token usage
//! (`compacting`). A compaction either harness makes is Compacting.
//!
//! The event is recorded through Operation (`Record.Caused`) for the seat
//! the flow runs at, at the moment Flow received it.

use crate::RunningNexus;
use crate::claude::MeasuresClaudeContext;
use crate::generated::memory::{Cause, Event, Moment};
use crate::generated::operation::{Operation, Outcome, Record_Data};
use crate::generated::overriding::{RemembersOverrides, Subject};
use crate::generated::transitions::{
    Caused, ClaudeReport, CodexFlowReport, CodexReport, ReportedEvent, ReportsEvents,
    TypesCodexReport,
};
use crate::generated::turn_ledger::ThreadStanding;
use crate::performing::Performs;
use crate::reminding::CountsUnixSeconds;
use crate::store::ReadsFlowRows;
use crate::store::context::RecordsContextSize;

use crate::waking::{CorrelatesNativeEvents, ReadsClaudeDelivery};
use signal_flow::{HarnessKind, Tokens};

impl TypesCodexReport for CodexReport {
    /// A closed thread is Ended. A batch that brings the thread to its rest
    /// is Idle, and one that takes it from anything else into a turn or a
    /// running item is Working, each with the size given; any other batch
    /// changes nothing. A token usage is Working with its size while the
    /// thread is anywhere but at its rest, and Idle with it there.
    fn codex_event(&self, last: Option<Tokens>) -> Option<Event> {
        let tokens = last.unwrap_or(Tokens(0));
        let turning = |standing: &ThreadStanding| {
            matches!(
                standing,
                ThreadStanding::Working(_) | ThreadStanding::ItemsRunning(_)
            )
        };
        match self {
            CodexReport::Closed => Some(Event::Ended),
            CodexReport::Stepped(step) => {
                if step.after.0 == ThreadStanding::AtRest && step.before.0 != ThreadStanding::AtRest
                {
                    Some(Event::Idle(tokens))
                } else if turning(&step.after.0) && !turning(&step.before.0) {
                    Some(Event::Working(tokens))
                } else {
                    None
                }
            }
            CodexReport::Measured(measured) => Some(match measured.thread_standing {
                ThreadStanding::AtRest => Event::Idle(measured.tokens),
                _ => Event::Working(measured.tokens),
            }),
        }
    }
}

impl ReportsEvents for RunningNexus {
    fn claude_event(&self, report: ClaudeReport) -> Option<Event> {
        let ClaudeReport {
            flow_id,
            hook_event,
        } = report;
        let node = self.store.flow_node(&flow_id.0).ok().flatten()?;
        if node.harness_kind != HarnessKind::Claude {
            return None;
        }
        let last = || {
            Tokens(
                self.store
                    .context_size(&flow_id.0)
                    .ok()
                    .flatten()
                    .unwrap_or(0),
            )
        };
        let measured = || {
            self.claude_transcript(&node)
                .and_then(|transcript| transcript.context_tokens())
                .map(Tokens)
                .unwrap_or_else(last)
        };
        match hook_event {
            // Session attachment is not a confirmed Nexus launch or an activity event.
            signal_flow::Event::Started(_) => None,
            signal_flow::Event::PromptSubmitted(prompt) => self
                .correlated_prompt(&flow_id.0, &prompt)
                .then(|| Event::Working(last())),
            signal_flow::Event::ToolUsed(_) => Some(Event::Working(last())),
            signal_flow::Event::Stopped(stop) => self
                .correlated_stop(&flow_id.0, &stop)
                .then(|| Event::Idle(measured())),
            signal_flow::Event::Ended(session) => (!node.session_id.0.is_empty()
                && session.as_ref().map(|session| session.0.as_str())
                    == Some(node.session_id.0.as_str()))
            .then_some(Event::Ended),
            // A cleared session goes on in the session the next Started
            // names; the clear itself is no event.
            signal_flow::Event::Cleared(_) => None,
        }
    }

    fn report_event(&self, reported: ReportedEvent) -> bool {
        let ReportedEvent { flow_id, event } = reported;
        let seat = match self.store.seat(Subject::Named(flow_id.clone())) {
            Ok(Some(seat)) => seat,
            Ok(None) => return false,
            Err(error) => {
                eprintln!(
                    "flow-nexus: flow {flow_id} has its {event:?} unrecorded: {error}",
                    flow_id = flow_id.0
                );
                return false;
            }
        };
        let caused = Caused {
            seat,
            cause: Cause::Reported(event),
            moment: Moment(std::time::SystemTime::now().seconds_since_epoch()),
        };
        match self.perform(Operation::Record(Record_Data::Caused(caused))) {
            Outcome::Recorded => true,
            outcome => {
                eprintln!(
                    "flow-nexus: flow {flow_id} has a reported event unrecorded: {outcome:?}",
                    flow_id = flow_id.0
                );
                false
            }
        }
    }

    fn report_codex(&self, report: CodexFlowReport) -> bool {
        let CodexFlowReport {
            flow_id,
            codex_report,
        } = report;
        let last = self
            .store
            .context_size(&flow_id.0)
            .ok()
            .flatten()
            .map(Tokens);
        let event = match codex_report {
            // Token accounting never changes the flow's activity.
            CodexReport::Measured(_) => None,
            other => other.codex_event(last),
        };
        match event {
            Some(event) => self.report_event(ReportedEvent { flow_id, event }),
            None => false,
        }
    }
}
