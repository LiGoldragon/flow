//! The sole decision state of a flow. No observation is a different result
//! from an observed end. Opening a store never infers this row from history.
use super::transitions::StagesObservationAudit;
use super::{FlowStore, StoreError};
use crate::generated::memory::{CurrentSeat, SeatActivity, SeatStanding, StateRevision};
use sema_engine::{
    AtomicCommit, EngineRecord, FamilyName, QueryPlan, RecordKey, SchemaHash, TableDescriptor,
    TableName,
};
use signal_flow::{BackgroundWork, FlowId};

pub(super) const TABLE: &str = "flow_nexus_current_seats";
pub(super) const FAMILY: &str = "flow-nexus-current-seat";
pub(super) const SCHEMA: &str = "flow-nexus-current-seat-v2";

pub(super) trait DescribesCurrentSeats {
    fn descriptor() -> TableDescriptor<CurrentSeat>;
}

impl DescribesCurrentSeats for CurrentSeat {
    fn descriptor() -> TableDescriptor<CurrentSeat> {
        TableDescriptor::new(
            TableName::new(TABLE),
            FamilyName::new(FAMILY),
            SchemaHash::for_label(SCHEMA),
        )
    }
}

impl EngineRecord for CurrentSeat {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.0.clone())
    }
}

pub(crate) struct KnownSeat {
    pub seat_standing: SeatStanding,
    pub state_revision: StateRevision,
    pub background_work: BackgroundWork,
}

pub(crate) enum CurrentSeatRead {
    NoRecord,
    Known(KnownSeat),
}

/// Decision view of the keyed record. An unavailable source cannot be
/// mistaken for the terminal state or for a store error.
pub(crate) enum SeatDecision {
    NoRecord,
    Observed(SeatStanding),
}

pub(crate) enum SeatEvent {
    Observed(SeatStanding),
    NewIncarnation(SeatStanding),
    Started,
    Activity(SeatActivity),
    Stopped(BackgroundWork),
    Ended,
}

pub(crate) trait RecordsCurrentSeats {
    fn seat_decision(&self, id: &str) -> Result<SeatDecision, StoreError>;
    fn seat_record(&self, id: &str) -> Result<Option<CurrentSeat>, StoreError>;
    fn current_seat(&self, id: &str) -> Result<CurrentSeatRead, StoreError>;
    fn record_observation(
        &self,
        id: &str,
        observation: Option<SeatStanding>,
    ) -> Result<(), StoreError>;
    fn establish_observation(&self, id: &str, standing: SeatStanding) -> Result<bool, StoreError>;
    fn stage_observation(
        &self,
        commit: AtomicCommit,
        id: &str,
        standing: SeatStanding,
    ) -> Result<AtomicCommit, StoreError>;
    fn stage_current_event(
        &self,
        commit: AtomicCommit,
        id: &str,
        event: SeatEvent,
    ) -> Result<AtomicCommit, StoreError>;
}

impl RecordsCurrentSeats for FlowStore {
    fn seat_decision(&self, id: &str) -> Result<SeatDecision, StoreError> {
        Ok(match self.current_seat(id)? {
            CurrentSeatRead::NoRecord => SeatDecision::NoRecord,
            CurrentSeatRead::Known(row) => SeatDecision::Observed(row.seat_standing),
        })
    }

    fn current_seat(&self, id: &str) -> Result<CurrentSeatRead, StoreError> {
        Ok(match self.seat_record(id)? {
            Some(CurrentSeat {
                seat_standing_option: Some(seat_standing),
                state_revision,
                background_work,
                ..
            }) => CurrentSeatRead::Known(KnownSeat {
                seat_standing,
                state_revision,
                background_work,
            }),
            _ => CurrentSeatRead::NoRecord,
        })
    }

    fn seat_record(&self, id: &str) -> Result<Option<CurrentSeat>, StoreError> {
        let rows = self
            .engine
            .match_records(QueryPlan::key(self.current_seats, RecordKey::new(id)))?;
        match rows.records() {
            [] => Ok(None),
            [row] => Ok(Some(row.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    /// A fresh authoritative observation replaces a prior known state. If the
    /// source cannot answer, remove stale knowledge rather than invent an end.
    fn record_observation(
        &self,
        id: &str,
        observation: Option<SeatStanding>,
    ) -> Result<(), StoreError> {
        let _gate = self
            .transitions_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // No transition means no audit entry and no Sema transaction.
        if let Some(next) = &observation
            && matches!(self.current_seat(id)?, CurrentSeatRead::Known(row) if row.seat_standing==SeatStanding::Ended || &row.seat_standing==next)
        {
            return Ok(());
        }
        let commit = self.engine.begin_atomic_commit();
        let commit = match observation {
            Some(standing) => self.stage_observation(commit, id, standing)?,
            None => match self.seat_record(id)? {
                Some(mut row)
                    if matches!(row.seat_standing_option, Some(SeatStanding::Alive(_))) =>
                {
                    if row.background_work.0.is_empty() {
                        commit.retract(self.current_seats, RecordKey::new(id))
                    } else {
                        // Unavailable is not evidence that authenticated jobs ended.
                        row.seat_standing_option = None;
                        row.state_revision.0 = row
                            .state_revision
                            .0
                            .checked_add(1)
                            .ok_or(StoreError::StateInvariant)?;
                        commit.mutate(self.current_seats, row)
                    }
                }
                _ => return Ok(()),
            },
        };
        self.engine.commit_atomic(commit)?;
        Ok(())
    }

    /// The source answered after an unavailable takeover. Do not overwrite
    /// a newer event that won the race while this observation was in flight.
    fn establish_observation(&self, id: &str, standing: SeatStanding) -> Result<bool, StoreError> {
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
    fn stage_current_event(
        &self,
        commit: AtomicCommit,
        id: &str,
        event: SeatEvent,
    ) -> Result<AtomicCommit, StoreError> {
        use SeatStanding::{Alive, Ended};
        let previous = self.seat_record(id)?;
        let standing = previous
            .as_ref()
            .and_then(|row| row.seat_standing_option.as_ref());
        let jobs = previous
            .as_ref()
            .map(|row| row.background_work.clone())
            .unwrap_or_else(|| BackgroundWork(vec![]));
        let (next, jobs) = match (standing, event) {
            (_, SeatEvent::NewIncarnation(next)) => (Some(next), BackgroundWork(vec![])),
            (Some(Ended), _) => return Ok(commit),
            (_, SeatEvent::Ended | SeatEvent::Observed(Ended)) => {
                (Some(Ended), BackgroundWork(vec![]))
            }
            (_, SeatEvent::Observed(next)) => (Some(next), jobs),
            (None, SeatEvent::Started) => (Some(Alive(SeatActivity::Working)), jobs),
            (Some(_), SeatEvent::Activity(activity)) => (Some(Alive(activity)), jobs),
            (Some(_), SeatEvent::Stopped(reported)) => {
                (Some(Alive(SeatActivity::Resting)), reported)
            }
            (None, SeatEvent::Stopped(reported))
                if !reported.0.is_empty() || previous.is_some() =>
            {
                (None, reported)
            }
            _ => return Ok(commit),
        };
        Ok(match previous {
            None => commit.assert(
                self.current_seats,
                CurrentSeat {
                    flow_id: FlowId(id.into()),
                    seat_standing_option: next,
                    state_revision: StateRevision(1),
                    background_work: jobs,
                },
            ),
            Some(_) if next.is_none() && jobs.0.is_empty() => {
                commit.retract(self.current_seats, RecordKey::new(id))
            }
            Some(mut row) if row.seat_standing_option != next || row.background_work != jobs => {
                row.seat_standing_option = next;
                row.background_work = jobs;
                row.state_revision.0 = row
                    .state_revision
                    .0
                    .checked_add(1)
                    .ok_or(StoreError::StateInvariant)?;
                commit.mutate(self.current_seats, row)
            }
            Some(_) => commit,
        })
    }
}
