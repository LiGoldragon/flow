//! Observe.Agent: a flow's agent state as Herdr shows it, on open and then
//! on every change Herdr announces, until the flow's pane is gone.
//!
//! The subscription is Herdr's own `events.subscribe` on the session's
//! socket (newline-delimited JSON): `pane.agent_status_changed` for the
//! flow's pane, and `pane.closed` / `pane.exited`. Nothing is re-read on a
//! timer; a flow whose agent does not change sends nothing.
//!
//! A pane Herdr shows gone, or announces closed, is a witness of the flow's
//! fate: a live flow whose pane is gone is recorded Exited, which puts its
//! metaflow Asleep and retracts every lock it holds as a sender, and a
//! Maintained metaflow so ended is woken into its successor.

use crate::RunningNexus;
use crate::herdr::HerdrCli;
use crate::herdr::events::{HerdrCue, HerdrEvents, SubscribesHerdrEvents};
use crate::herdr::pane::ReadsAgentStatus;
use crate::herdr::pane::{PaneAgent, WritesPane};
use crate::observation_lifetime::{CancelsObservation, ObservationCancellation};
use crate::store::{NamesLiveFlow, ReadsFlowRows};
use signal_flow::{
    AgentObservation, AgentState, FlowNode, HerdrRoute, HerdrRouteSelection, Response,
};
use std::path::PathBuf;

/// Answers and streams a flow's agent state.
pub trait ObservesAgent {
    /// The state now: the opening frame of the subscription.
    fn agent_observation(&self, flow_id: &str) -> Response;
    /// Sends the state now, then each change, ending after Gone.
    fn observe_agent(
        &self,
        flow_id: &str,
        cancellation: &ObservationCancellation,
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
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
            if !matches!(self.store.flow_node(flow_id), Ok(Some(node)) if node.flow_lifecycle.is_live())
            {
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
    fn observed_of(self, flow_id: &str) -> Response;
}

impl ReportsAgentState for AgentState {
    fn observed_of(self, flow_id: &str) -> Response {
        Response::AgentObserved(AgentObservation {
            flow_id: signal_flow::FlowId(flow_id.to_owned()),
            agent_state: self,
        })
    }
}

trait FindsObservableFlow {
    /// The live, routed flow, or None when it has nothing to observe.
    fn observable(&self, flow_id: &str) -> Option<(FlowNode, HerdrRoute)>;
}

impl FindsObservableFlow for RunningNexus {
    fn observable(&self, flow_id: &str) -> Option<(FlowNode, HerdrRoute)> {
        let node = self.store.flow_node(flow_id).ok()??;
        if !node.flow_lifecycle.is_live() {
            return None;
        }
        let HerdrRouteSelection::Available(route) = node.herdr_route_selection.clone() else {
            return None;
        };
        Some((node, route))
    }
}

impl ObservesAgent for RunningNexus {
    fn agent_observation(&self, flow_id: &str) -> Response {
        let Some((node, _)) = self.observable(flow_id) else {
            return AgentState::Gone.observed_of(flow_id);
        };
        let agent_state = match self.herdr.pane_agent(&node) {
            PaneAgent::Present { agent_state, .. } => agent_state,
            PaneAgent::Absent => {
                self.exited_as_witnessed(flow_id);
                AgentState::Gone
            }
            PaneAgent::Unreadable => AgentState::Unknown,
        };
        agent_state.observed_of(flow_id)
    }

    fn observe_agent(
        &self,
        flow_id: &str,
        cancellation: &ObservationCancellation,
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
    ) -> Result<(), String> {
        cancellation.check()?;
        let opening = self.agent_observation(flow_id);
        cancellation.check()?;
        send(&opening)?;
        let Response::AgentObserved(AgentObservation {
            agent_state: mut last,
            ..
        }) = opening
        else {
            return Ok(());
        };
        if last == AgentState::Gone {
            return Ok(());
        }
        let Some((_, route)) = self.observable(flow_id) else {
            return send(&AgentState::Gone.observed_of(flow_id));
        };
        // What this subscription watches: a Gone it reads acts only while
        // the flow still runs there, as the seat it was opened on.
        let witness = {
            use crate::restarting::FencesSeatEvents;
            self.seat_witness(flow_id)
        };
        let socket = self
            .herdr
            .session_socket(&route.herdr_session_name.0)
            .ok_or("Herdr session socket not found")?;
        let mut events = PaneEvents::subscribe(&socket, &route.herdr_pane_id.0, cancellation)?;
        loop {
            let Some(event) = events.next_event(cancellation)? else {
                // Herdr closed the subscription: the session is gone. The
                // pane is read once more before the flow is recorded ended.
                if let Some((node, _)) = self.observable(flow_id)
                    && matches!(self.herdr.pane_agent(&node), PaneAgent::Absent)
                {
                    self.exited_as_witnessed_by(flow_id, witness.as_ref());
                }
                return send(&AgentState::Gone.observed_of(flow_id));
            };
            let agent_state = match event {
                PaneEvent::Status(status) => Some(status.as_str()).agent_state(),
                PaneEvent::Gone => AgentState::Gone,
            };
            if agent_state == last {
                continue;
            }
            send(&agent_state.clone().observed_of(flow_id))?;
            if agent_state == AgentState::Gone {
                self.exited_as_witnessed_by(flow_id, witness.as_ref());
                return Ok(());
            }
            last = agent_state;
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
enum PaneEvent {
    Status(String),
    Gone,
}

/// The observed pane's stream: Herdr's events of it, waited on with its
/// connection's cancellation.
struct PaneEvents {
    pane_id: String,
    events: HerdrEvents,
}

trait ReadsPaneEvents: Sized {
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
        let events = HerdrEvents::subscribe(
            socket,
            "flow-nexus:observe-agent",
            serde_json::json!([
                {"type": "pane.agent_status_changed", "pane_id": pane_id},
                {"type": "pane.closed"},
                {"type": "pane.exited"}
            ]),
            None,
            Some(cancellation),
        )?;
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
            match self.events.next_cue(&[], None, Some(cancellation))? {
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
trait ReadsPaneEvent {
    /// Herdr 0.8.2 names events `pane_closed` on the stream and
    /// `pane.closed` in its schema; both spellings are read.
    fn pane_event(&self, pane_id: &str) -> Option<PaneEvent>;
}

impl ReadsPaneEvent for serde_json::Value {
    fn pane_event(&self, pane_id: &str) -> Option<PaneEvent> {
        let event = self;
        let data = event.get("data")?;
        if data.get("pane_id").and_then(serde_json::Value::as_str) != Some(pane_id) {
            return None;
        }
        let kind = event.get("event").and_then(serde_json::Value::as_str)?;
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
