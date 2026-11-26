//! The sole decision state of a flow. No observation is a different result
//! from an observed end. Opening a store never infers this row from history.
use super::{FlowStore, StoreError};
use crate::generated::memory::{CurrentSeat, SeatActivity, SeatStanding, StateRevision};
use sema_engine::{
    AtomicCommit, EngineRecord, FamilyName, QueryPlan, RecordKey, SchemaHash, TableDescriptor,
    TableName,
};
use signal_flow::FlowId;

pub(super) const TABLE: &str = "flow_nexus_current_seats";
pub(super) const FAMILY: &str = "flow-nexus-current-seat";
pub(super) const SCHEMA: &str = "flow-nexus-current-seat-v1";

pub(super) fn descriptor() -> TableDescriptor<CurrentSeat> {
    TableDescriptor::new(
        TableName::new(TABLE),
        FamilyName::new(FAMILY),
        SchemaHash::for_label(SCHEMA),
    )
}

impl EngineRecord for CurrentSeat {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.0.clone())
    }
}

pub(crate) enum CurrentSeatRead {
    NoRecord,
    Known(CurrentSeat),
}

/// Decision view of the keyed record. An unavailable source cannot be
/// mistaken for the terminal state or for a store error.
pub(crate) enum SeatDecision {
    NoRecord,
    Observed(SeatStanding),
}

pub(crate) enum SeatEvent {
    Observed(SeatStanding),
    Started,
    Activity(SeatActivity),
    Ended,
}

impl FlowStore {
    pub(crate) fn seat_decision(&self, id: &str) -> Result<SeatDecision, StoreError> {
        Ok(match self.current_seat(id)? {
            CurrentSeatRead::NoRecord => SeatDecision::NoRecord,
            CurrentSeatRead::Known(row) => SeatDecision::Observed(row.seat_standing),
        })
    }

    pub(crate) fn current_seat(&self, id: &str) -> Result<CurrentSeatRead, StoreError> {
        let rows = self
            .engine
            .match_records(QueryPlan::key(self.current_seats, RecordKey::new(id)))?;
        match rows.records() {
            [] => Ok(CurrentSeatRead::NoRecord),
            [row] => Ok(CurrentSeatRead::Known(row.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    /// A fresh authoritative observation replaces a prior known state. If the
    /// source cannot answer, remove stale knowledge rather than invent an end.
    pub(crate) fn record_observation(
        &self,
        id: &str,
        observation: Option<SeatStanding>,
    ) -> Result<(), StoreError> {
        let _gate = self
            .transitions_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let commit = self.engine.begin_atomic_commit();
        let commit = match observation {
            Some(standing) => self.stage_observation(commit, id, standing)?,
            None => match self.current_seat(id)? {
                CurrentSeatRead::Known(CurrentSeat {
                    seat_standing: SeatStanding::Alive(_),
                    ..
                }) => commit.retract(self.current_seats, RecordKey::new(id)),
                _ => return Ok(()),
            },
        };
        self.engine.commit_atomic(commit)?;
        Ok(())
    }

    /// The source answered after an unavailable takeover. Do not overwrite
    /// a newer event that won the race while this observation was in flight.
    pub(crate) fn establish_observation(
        &self,
        id: &str,
        standing: SeatStanding,
    ) -> Result<bool, StoreError> {
        let _gate = self
            .transitions_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if !matches!(self.current_seat(id)?, CurrentSeatRead::NoRecord) {
            return Ok(false);
        }
        let commit = self.stage_observation(self.engine.begin_atomic_commit(), id, standing)?;
        self.engine.commit_atomic(commit)?;
        Ok(true)
    }

    fn stage_observation(
        &self,
        commit: AtomicCommit,
        id: &str,
        standing: SeatStanding,
    ) -> Result<AtomicCommit, StoreError> {
        use crate::store::metaflow::SleepsMetaflows;
        if matches!(self.current_seat(id)?, CurrentSeatRead::Known(row) if row.seat_standing == standing || row.seat_standing == SeatStanding::Ended)
        {
            return Ok(commit);
        }
        let commit = match self.metaflow_awake_as(id)? {
            Some(seat) => self.stage_observation_audit(
                commit,
                crate::generated::overriding::Seat {
                    flow_id: FlowId(id.into()),
                    address: seat.address,
                },
                &standing,
            )?,
            None => commit,
        };
        self.stage_current_event(commit, id, SeatEvent::Observed(standing))
    }

    /// Called under transitions_gate and committed with the event source.
    pub(super) fn stage_current_event(
        &self,
        commit: AtomicCommit,
        id: &str,
        event: SeatEvent,
    ) -> Result<AtomicCommit, StoreError> {
        use CurrentSeatRead::{Known, NoRecord};
        use SeatStanding::{Alive, Ended};
        let previous = self.current_seat(id)?;
        let next = match (&previous, event) {
            (
                Known(CurrentSeat {
                    seat_standing: Ended,
                    ..
                }),
                _,
            ) => return Ok(commit),
            (_, SeatEvent::Observed(standing)) => standing,
            (_, SeatEvent::Ended) => Ended,
            (NoRecord, SeatEvent::Started) => Alive(SeatActivity::Working),
            (Known(_), SeatEvent::Started) | (NoRecord, SeatEvent::Activity(_)) => {
                return Ok(commit);
            }
            (Known(_), SeatEvent::Activity(activity)) => Alive(activity),
        };
        Ok(match previous {
            NoRecord => commit.assert(
                self.current_seats,
                CurrentSeat {
                    flow_id: FlowId(id.into()),
                    seat_standing: next,
                    state_revision: StateRevision(1),
                },
            ),
            Known(mut row) if row.seat_standing != next => {
                row.seat_standing = next;
                row.state_revision.0 = row
                    .state_revision
                    .0
                    .checked_add(1)
                    .ok_or(StoreError::StateInvariant)?;
                commit.mutate(self.current_seats, row)
            }
            Known(_) => commit,
        })
    }
}
