//! Append-only transition audit and the separate durable current-state read.

use super::current_seat::{CurrentSeatRead, SeatEvent};
use super::{FlowStore, ReadsFlowStore, StoreError};
use crate::generated::memory::{Cause, Event, Flow, Moment, Transition};
use crate::generated::memory::{SeatActivity, SeatStanding};
use crate::generated::overriding::Seat;
use crate::generated::transitions::LeadsToState;
use crate::generated::transitions::{
    Arrival, AskedFlow, Caused, ReceiptTurn, RecordsTransitions, TransitionWrite,
};
use crate::reminding::CountsUnixSeconds;
use crate::store::context::RecordsContextSize;
use meta_signal_flow::State;
use sema_engine::{QueryPlan, RecordKey};
use signal_flow::Tokens;

impl LeadsToState for Cause {
    /// Started and Working lead to Acting, Idle to Resting, Ended to
    /// Asleep, and Compacting keeps the state before it, Acting when none
    /// was held; an arrival leads to Acting. The causes this release
    /// decides no state by lead to none.
    fn leads_to(&self, before: Option<State>) -> Option<State> {
        match self {
            Cause::Reported(Event::Started | Event::Working(_)) | Cause::Arrived(_) => {
                Some(State::Acting)
            }
            Cause::Reported(Event::Idle(_)) => Some(State::Resting),
            Cause::Reported(Event::Ended) => Some(State::Asleep),
            Cause::Reported(Event::Compacting) => Some(before.unwrap_or(State::Acting)),
            Cause::CoolingDue | Cause::Crystallized(_) | Cause::Overridden => None,
        }
    }
}

impl LeadsToState for Caused {
    /// Where its cause leads.
    fn leads_to(&self, before: Option<State>) -> Option<State> {
        self.cause.leads_to(before)
    }
}

impl LeadsToState for Arrival {
    /// The fold rule, placed here once: an arrival whose receipt lies in a
    /// turn that has already ended records no Acting, and the seat stays
    /// as it is, at the state before or at none when none was held. Its
    /// rest came first, by whatever order the events reached Flow: a Stop's
    /// or a turn's completion before the settlement, or a restart between
    /// them. An arrival whose turn runs leads where Arrived does.
    fn leads_to(&self, before: Option<State>) -> Option<State> {
        match self.receipt_turn {
            ReceiptTurn::Ended => before,
            ReceiptTurn::Running => self.caused.leads_to(before),
        }
    }
}

/// What the fold appends to its seat's flow: one cause, and the state it
/// leads to from the state before.
trait FoldsIntoTransition: LeadsToState {
    fn caused(&self) -> &Caused;
}

impl FoldsIntoTransition for Caused {
    fn caused(&self) -> &Caused {
        self
    }
}

impl FoldsIntoTransition for Arrival {
    fn caused(&self) -> &Caused {
        &self.caused
    }
}

/// The store's one fold of causes into their flows' transitions, which
/// `record_caused` and `arrivals` share.
trait FoldsTransitions {
    /// One write per flow, each cause appended in order to what the flow's
    /// record holds, or to what an earlier cause of the same call wrote. A
    /// cause that leads to no state writes nothing.
    fn fold<Folded: FoldsIntoTransition>(
        &self,
        causes: Vec<Folded>,
    ) -> Result<Vec<TransitionWrite>, StoreError>;
}

impl FoldsTransitions for FlowStore {
    fn fold<Folded: FoldsIntoTransition>(
        &self,
        causes: Vec<Folded>,
    ) -> Result<Vec<TransitionWrite>, StoreError> {
        let mut writes: Vec<TransitionWrite> = Vec::new();
        for folded in causes {
            let caused = folded.caused();
            let flow_id = caused.seat.flow_id.clone();
            let position = writes.iter().position(|write| match write {
                TransitionWrite::Appended(flow) | TransitionWrite::Opened(flow) => {
                    flow.flow_id == flow_id
                }
            });
            let mut write = match position {
                Some(position) => writes.remove(position),
                None => match self
                    .engine
                    .match_records(QueryPlan::key(
                        self.flow_transitions,
                        RecordKey::new(flow_id.0.clone()),
                    ))?
                    .records()
                    .first()
                    .cloned()
                {
                    Some(flow) => TransitionWrite::Appended(flow),
                    None => TransitionWrite::Opened(Flow {
                        flow_id: flow_id.clone(),
                        address: caused.seat.address.clone(),
                        transition_vector: Vec::new(),
                    }),
                },
            };
            let flow = match &mut write {
                TransitionWrite::Appended(flow) | TransitionWrite::Opened(flow) => flow,
            };
            let before = flow.transition_vector.last().map(|last| last.state.clone());
            let Some(state) = folded.leads_to(before) else {
                // Nothing appended: a record this call already wrote stays
                // written, and one it only read is not written again.
                if position.is_some() {
                    writes.push(write);
                }
                continue;
            };
            flow.address = caused.seat.address.clone();
            flow.transition_vector.push(Transition {
                state,
                cause: caused.cause.clone(),
                moment: caused.moment,
            });
            writes.push(write);
        }
        Ok(writes)
    }
}

impl FlowStore {
    /// An observed native state is itself an event, in the same audit as a
    /// report. The caller holds the current-state writer gate across both.
    pub(super) fn stage_observation_audit(
        &self,
        commit: sema_engine::AtomicCommit,
        seat: Seat,
        standing: &SeatStanding,
    ) -> Result<sema_engine::AtomicCommit, StoreError> {
        let tokens = Tokens(self.context_size(&seat.flow_id.0)?.unwrap_or(0));
        let event = match standing {
            SeatStanding::Alive(SeatActivity::Working) => Event::Working(tokens),
            SeatStanding::Alive(SeatActivity::Resting) => Event::Idle(tokens),
            SeatStanding::Ended => Event::Ended,
        };
        let caused = Caused {
            seat,
            cause: Cause::Reported(event),
            moment: Moment(std::time::SystemTime::now().seconds_since_epoch()),
        };
        let writes = self.fold(vec![caused])?;
        Ok(writes
            .into_iter()
            .fold(commit, |commit, write| match write {
                TransitionWrite::Appended(flow) => commit.mutate(self.flow_transitions, flow),
                TransitionWrite::Opened(flow) => commit.assert(self.flow_transitions, flow),
            }))
    }
}

pub(crate) trait ReadsRestEpoch {
    /// A stable identity within the append-only history of this flow.
    fn rest_epoch(&self, flow_id: &str) -> Result<Option<usize>, StoreError>;
}

impl ReadsRestEpoch for FlowStore {
    fn rest_epoch(&self, flow_id: &str) -> Result<Option<usize>, StoreError> {
        Ok(match self.current_seat(flow_id)? {
            CurrentSeatRead::Known(row)
                if row.seat_standing == SeatStanding::Alive(SeatActivity::Resting) =>
            {
                usize::try_from(row.state_revision.0).ok()
            }
            _ => None,
        })
    }
}

impl RecordsTransitions for FlowStore {
    fn record_caused(&self, caused: Caused) -> Result<Option<Transition>, StoreError> {
        let _gate = self
            .transitions_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let id = caused.seat.flow_id.0.clone();
        // Claude's raw hook and its decision state already committed together.
        let from_codex = self
            .flow(&id)?
            .is_some_and(|row| row.harness_kind == signal_flow::HarnessKind::Codex);
        let event = match &caused.cause {
            Cause::Reported(Event::Working(_)) if from_codex => {
                Some(SeatEvent::Activity(SeatActivity::Working))
            }
            Cause::Reported(Event::Idle(_)) if from_codex => {
                Some(SeatEvent::Activity(SeatActivity::Resting))
            }
            Cause::Reported(Event::Ended) if from_codex => Some(SeatEvent::Ended),
            // Only confirm_start commits Started. Audit arrivals and token
            // measurements are not independent authorities on activity.
            _ => None,
        };
        let transition = self.fold(vec![caused])?.into_iter().next().map(|write| {
            let transition = match &write {
                TransitionWrite::Appended(flow) | TransitionWrite::Opened(flow) => {
                    flow.transition_vector.last().cloned()
                }
            };
            (write, transition)
        });
        let Some((write, transition)) = transition else {
            return Ok(None);
        };
        let commit = self.engine.begin_atomic_commit();
        let commit = match write {
            TransitionWrite::Appended(flow) => commit.mutate(self.flow_transitions, flow),
            TransitionWrite::Opened(flow) => commit.assert(self.flow_transitions, flow),
        };
        let commit = match event {
            Some(event) => self.stage_current_event(commit, &id, event)?,
            None => commit,
        };
        self.engine.commit_atomic(commit)?;
        Ok(transition)
    }

    /// Each arrival folded as it leads (`LeadsToState for Arrival`), one
    /// write per flow.
    fn arrivals(&self, arrivals: Vec<Arrival>) -> Result<Vec<TransitionWrite>, StoreError> {
        self.fold(arrivals)
    }

    fn state_of(&self, asked: AskedFlow) -> Result<Option<State>, StoreError> {
        Ok(match self.current_seat(&asked.0.0)? {
            CurrentSeatRead::NoRecord => None,
            CurrentSeatRead::Known(row) => Some(match row.seat_standing {
                SeatStanding::Alive(SeatActivity::Working) => State::Acting,
                SeatStanding::Alive(SeatActivity::Resting) => State::Resting,
                SeatStanding::Ended => State::Asleep,
            }),
        })
    }
}
