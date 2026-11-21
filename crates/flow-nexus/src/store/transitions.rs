//! A flow's state is a record of events (`ethos/transitions.ethos`): each
//! cause is appended to its seat's flow as the transition it leads to, in
//! the family the override panel opened (`overriding`), and the state a flow
//! is in is its last transition's.
//!
//! Every write of that family takes `transitions_gate`, so two causes
//! recorded at once each read the record the other wrote. A delivery that
//! makes a request arrive takes the gate over its own commit and writes the
//! arrival in it (`logical_delivery`), folded with the turn its receipt
//! lies in: one whose turn has already ended records no Acting, and the
//! seat stays as it is (`LeadsToState for Arrival`).
//!
//! The Memory upgrade runs at every open (`seed_reported_transitions`): a
//! store an earlier Flow wrote holds each flow's reports as harness events
//! and no transition caused by one, so each live flow whose transitions hold
//! no Reported one is given the one its last recorded report maps to. A
//! flow the override panel already recorded keeps the overrides after it,
//! the owner's word standing last. Nothing a 0.56.2 or a 0.58.0 store holds
//! is refused, and no family or schema changes.

use super::{FlowStore, ReadsFlowStore, StoreError};
use crate::generated::memory::{Cause, Event, Flow, FlowEvents, Moment, Transition};
use crate::generated::overriding::{RemembersOverrides, Seat, Subject};
use crate::generated::transitions::LeadsToState;
use crate::generated::transitions::{
    Arrival, AskedFlow, Caused, ReceiptTurn, RecordsTransitions, SeedsReportedTransition,
    TransitionWrite,
};
use crate::reminding::CountsUnixSeconds;
use crate::store::NamesLiveFlow;
use crate::store::context::RecordsContextSize;
use crate::store::events::RecordedHarnessEvent;
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

impl SeedsReportedTransition for FlowEvents {
    /// Each recorded report maps as the Nexus maps it now, with the size
    /// given: Started, a tool boundary and a prompt Working, a Stop Idle,
    /// an end Ended. A Started of a session an earlier Started named is the
    /// session going on, and takes the event before it; a Cleared maps to
    /// none.
    fn last_reported(&self, tokens: Option<Tokens>) -> Option<Event> {
        let tokens = tokens.unwrap_or(Tokens(0));
        let events = &self.recorded_harness_event_vector;
        events
            .iter()
            .enumerate()
            .filter_map(|(index, event)| match event {
                RecordedHarnessEvent::Started(session) => {
                    let repeated = session.is_some()
                        && events[..index].iter().any(|earlier| {
                            matches!(earlier, RecordedHarnessEvent::Started(named) if named == session)
                        });
                    (!repeated).then_some(Event::Started)
                }
                RecordedHarnessEvent::ToolUsed(_) | RecordedHarnessEvent::PromptSubmitted(_) => {
                    Some(Event::Working(tokens))
                }
                RecordedHarnessEvent::Stopped(_) => Some(Event::Idle(tokens)),
                RecordedHarnessEvent::Ended(_) => Some(Event::Ended),
                RecordedHarnessEvent::Cleared(_) => None,
            })
            .next_back()
    }
}

pub(crate) trait ReadsRestEpoch {
    /// A stable identity within the append-only history of this flow.
    fn rest_epoch(&self, flow_id: &str) -> Result<Option<usize>, StoreError>;
}

impl ReadsRestEpoch for FlowStore {
    fn rest_epoch(&self, flow_id: &str) -> Result<Option<usize>, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.flow_transitions,
            RecordKey::new(flow_id.to_owned()),
        ))?;
        Ok(rows.records().first().and_then(|flow| {
            matches!(flow.transition_vector.last(), Some(last) if last.state == State::Resting)
                .then_some(flow.transition_vector.len())
        }))
    }
}

impl RecordsTransitions for FlowStore {
    fn record_caused(&self, caused: Caused) -> Result<Option<Transition>, StoreError> {
        let _gate = self
            .transitions_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
        self.engine.commit_atomic(commit)?;
        Ok(transition)
    }

    /// Each arrival folded as it leads (`LeadsToState for Arrival`), one
    /// write per flow.
    fn arrivals(&self, arrivals: Vec<Arrival>) -> Result<Vec<TransitionWrite>, StoreError> {
        self.fold(arrivals)
    }

    fn state_of(&self, asked: AskedFlow) -> Result<Option<State>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(
                self.flow_transitions,
                RecordKey::new(asked.0.0),
            ))?
            .records()
            .first()
            .and_then(|flow| flow.transition_vector.last())
            .map(|last| last.state.clone()))
    }

    /// Each live flow whose transitions hold no Reported one is given the
    /// transition its last recorded report maps to, carrying the context
    /// size Memory holds for it: appended, the moment now, to a flow with no
    /// transition; placed first, at its first override's moment, before
    /// the overrides a flow holds. A flow Memory seats nowhere is left as
    /// it is. Answers how many flows were given one.
    fn seed_reported_transitions(&self) -> Result<i64, StoreError> {
        let _gate = self
            .transitions_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = Moment(std::time::SystemTime::now().seconds_since_epoch());
        let recorded = self
            .engine
            .match_records(QueryPlan::all(self.event_tables.events))?
            .records()
            .to_vec();
        let mut seeded = 0;
        for events in recorded {
            let flow_id = events.flow_id.clone();
            if !self
                .flow(&flow_id.0)?
                .is_some_and(|row| row.flow_lifecycle.is_live())
            {
                continue;
            }
            let held = self
                .engine
                .match_records(QueryPlan::key(
                    self.flow_transitions,
                    RecordKey::new(flow_id.0.clone()),
                ))?
                .records()
                .first()
                .cloned();
            if held.as_ref().is_some_and(|flow| {
                flow.transition_vector
                    .iter()
                    .any(|transition| matches!(transition.cause, Cause::Reported(_)))
            }) {
                continue;
            }
            let tokens = self.context_size(&flow_id.0)?.map(Tokens);
            let Some(event) = events.last_reported(tokens) else {
                continue;
            };
            let cause = Cause::Reported(event);
            let Some(state) = cause.leads_to(None) else {
                continue;
            };
            let commit = self.engine.begin_atomic_commit();
            let commit = match held {
                Some(mut flow) => {
                    let moment = flow
                        .transition_vector
                        .first()
                        .map(|first| first.moment)
                        .filter(|first| first.0 < now.0)
                        .unwrap_or(now);
                    flow.transition_vector.insert(
                        0,
                        Transition {
                            state,
                            cause,
                            moment,
                        },
                    );
                    commit.mutate(self.flow_transitions, flow)
                }
                None => {
                    let Some(Seat { flow_id, address }) =
                        self.seat(Subject::Named(flow_id.clone()))?
                    else {
                        continue;
                    };
                    commit.assert(
                        self.flow_transitions,
                        Flow {
                            flow_id,
                            address,
                            transition_vector: vec![Transition {
                                state,
                                cause,
                                moment: now,
                            }],
                        },
                    )
                }
            };
            self.engine.commit_atomic(commit)?;
            seeded += 1;
        }
        Ok(seeded)
    }
}
