//! Observe.Agent projects changes of the authoritative current harness record.

use crate::RunningNexus;
use crate::herdr::HerdrCli;
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
                        agent_state: AgentState::Gone,
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
