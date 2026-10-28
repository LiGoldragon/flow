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

use crate::herdr::HerdrCli;
use crate::herdr::pane::ReadsAgentStatus;
use crate::herdr::pane::{PaneAgent, WritesPane};
use crate::store::{NamesLiveFlow, ReadsFlowRows};
use crate::{HoldsStopRequest, RunningNexus};
use signal_flow::{
    AgentObservation, AgentState, FlowNode, HerdrRoute, HerdrRouteSelection, Response,
};
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    path::PathBuf,
};

/// Answers and streams a flow's agent state.
pub trait ObservesAgent {
    /// The state now: the opening frame of the subscription.
    fn agent_observation(&self, flow_id: &str) -> Response;
    /// Sends the state now, then each change, ending after Gone.
    fn observe_agent(
        &self,
        flow_id: &str,
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
    ) -> Result<(), String>;
}

/// A live flow whose pane Herdr witnessed gone, or whose harness reported
/// its own session ended, has exited.
pub(crate) trait RecordsWitnessedExit {
    /// Records the flow Exited, under the dispatch gate, when it is still
    /// live; then wakes its metaflow when Flow's records show it due.
    fn exited_as_witnessed(&self, flow_id: &str);
}

impl RecordsWitnessedExit for RunningNexus {
    fn exited_as_witnessed(&self, flow_id: &str) {
        use crate::generated::operation::{Operation, Outcome, Record_Data};
        use crate::performing::Performs;
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
            let address = self
                .store
                .metaflow_awake_as(flow_id)
                .ok()
                .flatten()
                .map(|metaflow| metaflow.address);
            if self.perform_ingress(Operation::Record(Record_Data::Exited(flow_id.to_owned())))
                != Outcome::Recorded
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
            flow_id: flow_id.to_owned(),
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
        let configured = match self.configured_use() {
            Ok(configured) => configured,
            Err(_) => return AgentState::Unknown.observed_of(flow_id),
        };
        let agent_state = match configured.herdr.pane_agent(&node) {
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
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
    ) -> Result<(), String> {
        let opening = self.agent_observation(flow_id);
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
        let configured = self.configured_use().map_err(|error| error.to_string())?;
        let socket = configured
            .herdr
            .session_socket(&route.herdr_session_name)
            .ok_or("Herdr session socket not found")?;
        let mut events = HerdrEvents::subscribe(&socket, &route.herdr_pane_id)?;
        loop {
            // Each event is a new item for a long-lived observer and takes
            // the then-current committed generation whole.
            let configured = self.configured_use().map_err(|error| error.to_string())?;
            let poll_milliseconds = configured.snapshot.timings.agent_observe_poll;
            let event = events.next_event(
                &|| self.stopping.requested(),
                std::time::Duration::from_millis(poll_milliseconds as u64),
            )?;
            // Configuration may have committed while the event source was
            // blocked; processing this new item uses the new whole view.
            let configured = self.configured_use().map_err(|error| error.to_string())?;
            let Some(event) = event else {
                // Herdr closed the subscription: the session is gone. The
                // pane is read once more before the flow is recorded ended.
                if let Some((node, _)) = self.observable(flow_id)
                    && matches!(configured.herdr.pane_agent(&node), PaneAgent::Absent)
                {
                    self.exited_as_witnessed(flow_id);
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
                self.exited_as_witnessed(flow_id);
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

/// One open Herdr event subscription for one pane.
struct HerdrEvents {
    pane_id: String,
    reader: BufReader<UnixStream>,
    /// What has arrived of the line not yet ended.
    pending: Vec<u8>,
}

trait SubscribesHerdrEvents: Sized {
    fn subscribe(socket: &std::path::Path, pane_id: &str) -> Result<Self, String>;
    /// The next event about this pane; None when Herdr closes the stream,
    /// an error once `stopping` holds.
    fn next_event(
        &mut self,
        stopping: &dyn Fn() -> bool,
        stop_poll: std::time::Duration,
    ) -> Result<Option<PaneEvent>, String>;
}

impl SubscribesHerdrEvents for HerdrEvents {
    fn subscribe(socket: &std::path::Path, pane_id: &str) -> Result<Self, String> {
        let mut stream = UnixStream::connect(socket).map_err(|error| error.to_string())?;
        let request = serde_json::json!({
            "id": "flow-nexus:observe-agent",
            "method": "events.subscribe",
            "params": {"subscriptions": [
                {"type": "pane.agent_status_changed", "pane_id": pane_id},
                {"type": "pane.closed"},
                {"type": "pane.exited"}
            ]}
        });
        stream
            .write_all(format!("{request}\n").as_bytes())
            .map_err(|error| error.to_string())?;
        let mut events = Self {
            pane_id: pane_id.to_owned(),
            reader: BufReader::new(stream),
            pending: Vec::new(),
        };
        let mut started = String::new();
        events
            .reader
            .read_line(&mut started)
            .map_err(|error| error.to_string())?;
        let reply: serde_json::Value =
            serde_json::from_str(&started).map_err(|error| error.to_string())?;
        if reply
            .pointer("/result/type")
            .and_then(serde_json::Value::as_str)
            != Some("subscription_started")
        {
            return Err(format!("Herdr refused the subscription: {started}"));
        }
        Ok(events)
    }

    fn next_event(
        &mut self,
        stopping: &dyn Fn() -> bool,
        stop_poll: std::time::Duration,
    ) -> Result<Option<PaneEvent>, String> {
        self.reader
            .get_ref()
            .set_read_timeout(Some(stop_poll))
            .map_err(|error| error.to_string())?;
        loop {
            if stopping() {
                return Err("the Nexus is stopping".into());
            }
            // A timed-out read keeps what it read in `pending`.
            match self.reader.read_until(b'\n', &mut self.pending) {
                Ok(0) => return Ok(None),
                Ok(_) if self.pending.last() != Some(&b'\n') => return Ok(None),
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    continue;
                }
                Err(error) => return Err(error.to_string()),
            }
            let line = String::from_utf8_lossy(&std::mem::take(&mut self.pending)).into_owned();
            let Ok(event) = serde_json::from_str::<serde_json::Value>(&line) else {
                continue;
            };
            if let Some(event) = event.pane_event(&self.pane_id) {
                return Ok(Some(event));
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
