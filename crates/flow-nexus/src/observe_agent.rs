//! Observe.Agent: a flow's agent state as Herdr shows it, on open and then
//! on every change Herdr announces, until the flow's pane is gone.
//!
//! The subscription is Herdr's own `events.subscribe` on the session's
//! socket (newline-delimited JSON): `pane.agent_status_changed` for the
//! flow's pane, and `pane.closed` / `pane.exited`. Nothing is re-read on a
//! timer; a flow whose agent does not change sends nothing.

use crate::RunningNexus;
use crate::herdr::HerdrCli;
use crate::herdr::pane::{PaneAgent, WritesPane};
use crate::store::{NamesLiveFlow, ReadsFlowRows};
use signal_flow::{
    AgentObservation, AgentState, FlowNode, HerdrRoute, HerdrRouteSelection, Response,
};
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    path::PathBuf,
    process::Command,
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

impl RunningNexus {
    fn observed(flow_id: &str, agent_state: AgentState) -> Response {
        Response::AgentObserved(AgentObservation {
            flow_id: flow_id.to_owned(),
            agent_state,
        })
    }

    /// The live, routed flow, or None when it has nothing to observe.
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
            return Self::observed(flow_id, AgentState::Gone);
        };
        let agent_state = match self.herdr.pane_agent(&node) {
            PaneAgent::Present { agent_state, .. } => agent_state,
            PaneAgent::Absent => AgentState::Gone,
            PaneAgent::Unreadable => AgentState::Unknown,
        };
        Self::observed(flow_id, agent_state)
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
            return send(&Self::observed(flow_id, AgentState::Gone));
        };
        let socket = self
            .herdr
            .session_socket(&route.herdr_session_name)
            .ok_or("Herdr session socket not found")?;
        let mut events = HerdrEvents::subscribe(&socket, &route.herdr_pane_id)?;
        loop {
            let Some(event) = events.next_event()? else {
                // Herdr closed the subscription: the session is gone.
                return send(&Self::observed(flow_id, AgentState::Gone));
            };
            let agent_state = match event {
                PaneEvent::Status(status) => HerdrCli::agent_state_of(Some(&status)),
                PaneEvent::Gone => AgentState::Gone,
            };
            if agent_state == last {
                continue;
            }
            send(&Self::observed(flow_id, agent_state.clone()))?;
            if agent_state == AgentState::Gone {
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
    /// `herdr session list` prints one row per session: name, status,
    /// directory, socket.
    fn session_socket(&self, session_name: &str) -> Option<PathBuf> {
        let output = Command::new(&self.executable)
            .args(["session", "list"])
            .output()
            .ok()?;
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .skip(1)
            .find_map(|row| {
                let columns: Vec<&str> = row.split_whitespace().collect();
                (columns.first() == Some(&session_name))
                    .then(|| columns.last().map(PathBuf::from))
                    .flatten()
            })
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
}

impl HerdrEvents {
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

    /// The next event about this pane; None when Herdr closes the stream.
    fn next_event(&mut self) -> Result<Option<PaneEvent>, String> {
        loop {
            let mut line = String::new();
            if self
                .reader
                .read_line(&mut line)
                .map_err(|error| error.to_string())?
                == 0
            {
                return Ok(None);
            }
            let Ok(event) = serde_json::from_str::<serde_json::Value>(&line) else {
                continue;
            };
            if let Some(event) = PaneEvent::read(&event, &self.pane_id) {
                return Ok(Some(event));
            }
        }
    }
}

impl PaneEvent {
    /// Herdr 0.8.2 names events `pane_closed` on the stream and
    /// `pane.closed` in its schema; both spellings are read.
    fn read(event: &serde_json::Value, pane_id: &str) -> Option<Self> {
        let data = event.get("data")?;
        if data.get("pane_id").and_then(serde_json::Value::as_str) != Some(pane_id) {
            return None;
        }
        let kind = event.get("event").and_then(serde_json::Value::as_str)?;
        if kind.ends_with("closed") || kind.ends_with("exited") {
            return Some(Self::Gone);
        }
        data.get("agent_status")
            .and_then(serde_json::Value::as_str)
            .map(|status| Self::Status(status.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::PaneEvent;

    #[test]
    fn herdr_events_read_as_the_observed_panes_state_or_its_going() {
        // As Herdr 0.8.2 sent them on the stream, 2026-09-25.
        let closed = serde_json::json!({"data":{"pane_id":"w1:p3","type":"pane_closed","workspace_id":"w1"},"event":"pane_closed"});
        assert_eq!(PaneEvent::read(&closed, "w1:p3"), Some(PaneEvent::Gone));
        assert_eq!(PaneEvent::read(&closed, "w1:p1"), None);
        let working = serde_json::json!({"data":{"pane_id":"w1:p1","agent_status":"working","workspace_id":"w1"},"event":"pane_agent_status_changed"});
        assert_eq!(
            PaneEvent::read(&working, "w1:p1"),
            Some(PaneEvent::Status("working".into()))
        );
    }
}
