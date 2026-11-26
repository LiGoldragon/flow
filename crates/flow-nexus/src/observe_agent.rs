//! Observe.Agent projects changes of the authoritative current harness record.

use crate::RunningNexus;
use crate::herdr::HerdrCli;
use crate::herdr::events::{HerdrCue, HerdrEvents, SubscribesHerdrEvents};
use crate::observation_lifetime::{CancelsObservation, ObservationCancellation};
use crate::store::current_seat::RecordsCurrentSeats;
use crate::store::{AnnouncesLaunchChanges, metaflow_watch::WatchesMetaflows};
use signal_flow::{AgentObservation, AgentState, Output};
use std::path::PathBuf;

/// Answers and streams a flow's agent state.
pub trait ObservesAgent {
    /// The state now: the opening frame of the subscription.
    fn agent_observation(&self, flow_id: &str) -> Output;
    /// Sends the state now, then each change, ending after Gone.
    fn observe_agent(
        &self,
        flow_id: &str,
        cancellation: &ObservationCancellation,
        send: &mut dyn FnMut(&Output) -> Result<(), String>,
    ) -> Result<(), String>;
}

/// A live flow whose pane Herdr witnessed gone, or whose harness reported
/// its own session ended, has exited.
pub(crate) trait RecordsWitnessedExit {
    /// Records the flow Exited, under the dispatch gate, when it is still
    /// live; then wakes its metaflow when Flow's records show it due.
    fn exited_as_witnessed(&self, flow_id: &str) {
        self.exited_as_witnessed_by(flow_id, None);
    }
    /// As `exited_as_witnessed`, for an exit witnessed of `witness`: the
    /// route and seat birth an Observe was opened on. It is recorded only
    /// while the flow still runs there as that seat, and never while a
    /// restart holds it.
    fn exited_as_witnessed_by(
        &self,
        flow_id: &str,
        witness: Option<&crate::restarting::SeatWitness>,
    );
}

impl RecordsWitnessedExit for RunningNexus {
    fn exited_as_witnessed_by(
        &self,
        flow_id: &str,
        witness: Option<&crate::restarting::SeatWitness>,
    ) {
        use crate::generated::operation::{Operation, Outcome, Record_Data};
        use crate::performing::Performs;
        use crate::restarting::FencesSeatEvents;
        use crate::store::metaflow::SleepsMetaflows;
        use crate::waking::SleepsAndWakesSeats;
        let address = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !matches!(
                self.store.current_seat(flow_id),
                Ok(crate::store::current_seat::CurrentSeatRead::Known(_))
            ) {
                return;
            }
            if !self.witness_holds(flow_id, witness) {
                eprintln!(
                    "flow-nexus: flow {flow_id} was seen gone where it no longer runs; no exit is recorded"
                );
                return;
            }
            let address = self
                .store
                .metaflow_awake_as(flow_id)
                .ok()
                .flatten()
                .map(|metaflow| metaflow.address);
            if self.perform(Operation::Record(Record_Data::Exited(signal_flow::FlowId(
                flow_id.to_owned(),
            )))) != Outcome::Recorded
            {
                eprintln!("flow-nexus: flow {flow_id} seen gone but not recorded Exited");
                return;
            }
            address
        };
        if let Some(address) = address {
            self.wake_due(address);
        }
    }
}

/// An agent state is observed of one flow.
trait ReportsAgentState {
    fn observed_of(self, flow_id: &str) -> Output;
}

impl ReportsAgentState for AgentState {
    fn observed_of(self, flow_id: &str) -> Output {
        Output::AgentObserved(AgentObservation {
            flow_id: signal_flow::FlowId(flow_id.to_owned()),
            agent_state: self,
        })
    }
}

impl ObservesAgent for RunningNexus {
    fn agent_observation(&self, flow_id: &str) -> Output {
        use crate::generated::memory::{SeatActivity, SeatStanding};
        use crate::store::current_seat::SeatDecision;
        match self.store.seat_decision(flow_id) {
            Ok(SeatDecision::Observed(SeatStanding::Alive(SeatActivity::Working))) => {
                AgentState::Working
            }
            Ok(SeatDecision::Observed(SeatStanding::Alive(SeatActivity::Resting))) => {
                AgentState::Idle
            }
            Ok(SeatDecision::Observed(SeatStanding::Ended)) => AgentState::Gone,
            Ok(SeatDecision::NoRecord) | Err(_) => AgentState::Unknown,
        }
        .observed_of(flow_id)
    }

    fn observe_agent(
        &self,
        flow_id: &str,
        cancellation: &ObservationCancellation,
        send: &mut dyn FnMut(&Output) -> Result<(), String>,
    ) -> Result<(), String> {
        self.store
            .watch_metaflows()
            .map_err(|error| error.to_string())?;
        let changes = &self.store.launch_changes;
        let mut last = None;
        loop {
            let seen = changes.current();
            cancellation.check()?;
            let current = self.agent_observation(flow_id);
            if last.as_ref() != Some(&current) {
                send(&current)?;
                if matches!(
                    current,
                    Output::AgentObserved(AgentObservation {
                        agent_state: AgentState::Gone | AgentState::Unknown,
                        ..
                    })
                ) {
                    return Ok(());
                }
                last = Some(current);
            }
            changes.after_while(seen, &|| cancellation.cause().is_none());
        }
    }
}

/// Names the socket of a Herdr session.
pub trait LocatesHerdrSession {
    fn session_socket(&self, session_name: &str) -> Option<PathBuf>;
}

impl LocatesHerdrSession for HerdrCli {
    fn session_socket(&self, session_name: &str) -> Option<PathBuf> {
        use crate::caller::KnowsHerdrServers;
        self.herdr_sessions()?
            .into_iter()
            .find(|session| session.name == session_name)
            .map(|session| session.socket)
    }
}

/// What Herdr said of the observed pane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PaneEvent {
    Status(String),
    Updated,
    Gone,
}

/// The observed pane's stream: Herdr's events of it, waited on with its
/// connection's cancellation.
pub(crate) struct PaneEvents {
    pane_id: String,
    events: HerdrEvents,
}

pub(crate) trait ReadsPaneEvents: Sized {
    fn subscribe(
        socket: &std::path::Path,
        pane_id: &str,
        cancellation: &ObservationCancellation,
    ) -> Result<Self, String>;
    /// The next event about this pane; None when Herdr closes the stream,
    /// an error once the observation is cancelled. The stream and the
    /// cancellation descriptor are waited on together, so either wakes it.
    fn next_event(
        &mut self,
        cancellation: &ObservationCancellation,
    ) -> Result<Option<PaneEvent>, String>;
}

impl ReadsPaneEvents for PaneEvents {
    fn subscribe(
        socket: &std::path::Path,
        pane_id: &str,
        cancellation: &ObservationCancellation,
    ) -> Result<Self, String> {
        let subscriptions = vec![
            serde_json::json!({"type": "pane.agent_status_changed", "pane_id": pane_id}),
            serde_json::json!({"type": "pane.closed"}),
            serde_json::json!({"type": "pane.exited"}),
            serde_json::json!({"type": "pane.updated"}),
        ];
        let events = HerdrEvents::subscribe(
            socket,
            "flow-nexus:observe-agent",
            serde_json::Value::Array(subscriptions),
            None,
            Some(cancellation),
        )
        .map_err(|error| error.to_string())?;
        Ok(Self {
            pane_id: pane_id.to_owned(),
            events,
        })
    }

    fn next_event(
        &mut self,
        cancellation: &ObservationCancellation,
    ) -> Result<Option<PaneEvent>, String> {
        loop {
            cancellation.check()?;
            match self
                .events
                .next_cue(&[], None, Some(cancellation))
                .map_err(|error| error.to_string())?
            {
                HerdrCue::Event(event) => {
                    if let Some(event) = event.pane_event(&self.pane_id) {
                        return Ok(Some(event));
                    }
                }
                HerdrCue::Other => return Err("the Nexus is stopping".into()),
                HerdrCue::Closed => return Ok(None),
                HerdrCue::Passed => {}
            }
        }
    }
}

/// A Herdr stream event, read as what it says of one pane.
pub(crate) trait ReadsPaneEvent {
    /// Herdr 0.8.2 names events `pane_closed` on the stream and
    /// `pane.closed` in its schema; both spellings are read.
    fn pane_event(&self, pane_id: &str) -> Option<PaneEvent>;
}

impl ReadsPaneEvent for serde_json::Value {
    fn pane_event(&self, pane_id: &str) -> Option<PaneEvent> {
        let event = self;
        let data = event.get("data")?;
        let kind = event.get("event").and_then(serde_json::Value::as_str)?;
        if matches!(kind, "pane_updated" | "pane.updated") {
            return (data
                .pointer("/pane/pane_id")
                .and_then(serde_json::Value::as_str)
                == Some(pane_id))
            .then_some(PaneEvent::Updated);
        }
        if data.get("pane_id").and_then(serde_json::Value::as_str) != Some(pane_id) {
            return None;
        }
        if kind.ends_with("closed") || kind.ends_with("exited") {
            return Some(PaneEvent::Gone);
        }
        data.get("agent_status")
            .and_then(serde_json::Value::as_str)
            .map(|status| PaneEvent::Status(status.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::{PaneEvent, ReadsPaneEvent};

    #[test]
    fn herdr_events_read_as_the_observed_panes_state_or_its_going() {
        // As Herdr 0.8.2 sent them on the stream, 2026-09-25.
        let closed = serde_json::json!({"data":{"pane_id":"w1:p3","type":"pane_closed","workspace_id":"w1"},"event":"pane_closed"});
        assert_eq!(closed.pane_event("w1:p3"), Some(PaneEvent::Gone));
        assert_eq!(closed.pane_event("w1:p1"), None);
        let working = serde_json::json!({"data":{"pane_id":"w1:p1","agent_status":"working","workspace_id":"w1"},"event":"pane_agent_status_changed"});
        assert_eq!(
            working.pane_event("w1:p1"),
            Some(PaneEvent::Status("working".into()))
        );
    }
}
