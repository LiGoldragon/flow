//! A flow's Restart under way, and the seat a restart bound.
//!
//! The RestartRecord is the restart's durable lock on the flow's Address:
//! every admission reads it, and it survives a Nexus restart. Each phase is
//! written before the effect it names. The record goes in the one write that
//! binds the resumed seat: its route, its SeatBirth and the record's removal
//! together, so a Nexus that dies on either side of that write finds either
//! the record (and binds again by observation) or the bound seat (and hands
//! it its queue through the ordinary delivery custody), never both and
//! never neither.
//!
//! A SeatBirth stays after the record is gone: it is the route the bound
//! seat runs in and the birth of its harness process, by which a late event
//! of the seat it replaced is told from the seat's own.

use super::{AnnouncesLaunchChanges, FlowHerdrRouteRecord, FlowStore, ReadsFlowStore, StoreError};
use crate::generated::memory::SeatBirth;
use meta_signal_flow::{RestartPhase, RestartRecord};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    AtomicCommit, EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, QueryPlan, RecordKey,
    SchemaHash, TableDescriptor, TableName,
};
use signal_flow::Address;

pub(super) const RESTART_TABLE_NAME: TableName = TableName::new("flow_nexus_restarts");
pub(super) const RESTART_FAMILY: &str = "flow-nexus-restart";
pub(super) const RESTART_SCHEMA: &str = "flow-nexus-restart-v1";
pub(super) const SEAT_BIRTH_TABLE_NAME: TableName = TableName::new("flow_nexus_seat_births");
pub(super) const SEAT_BIRTH_FAMILY: &str = "flow-nexus-seat-birth";
pub(super) const SEAT_BIRTH_SCHEMA: &str = "flow-nexus-seat-birth-v1";

/// One flow's restart under way, keyed by its FlowId.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredRestart {
    pub restart_record: RestartRecord,
}

impl EngineRecord for StoredRestart {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.restart_record.flow_id.clone().0)
    }
}

impl MemoryDefinition for StoredRestart {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(RESTART_SCHEMA)
    }
}

impl EngineRecord for SeatBirth {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone().0)
    }
}

impl MemoryDefinition for SeatBirth {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(SEAT_BIRTH_SCHEMA)
    }
}

pub(super) trait DescribesRestartTables {
    fn restart_descriptor() -> TableDescriptor<StoredRestart>;
    fn seat_birth_descriptor() -> TableDescriptor<SeatBirth>;
}

impl DescribesRestartTables for FlowStore {
    fn restart_descriptor() -> TableDescriptor<StoredRestart> {
        TableDescriptor::new(
            RESTART_TABLE_NAME,
            FamilyName::new(RESTART_FAMILY),
            StoredRestart::schema_hash(),
        )
    }

    fn seat_birth_descriptor() -> TableDescriptor<SeatBirth> {
        TableDescriptor::new(
            SEAT_BIRTH_TABLE_NAME,
            FamilyName::new(SEAT_BIRTH_FAMILY),
            SeatBirth::schema_hash(),
        )
    }
}

/// The restart records and seat births Flow keeps.
pub(crate) trait RecordsRestarts {
    /// The restart of `flow_id` under way, if any.
    fn restart_record(&self, flow_id: &str) -> Result<Option<RestartRecord>, StoreError>;
    /// The restart under way of a flow at `address`, if any.
    fn restart_at(&self, address: &Address) -> Result<Option<RestartRecord>, StoreError>;
    /// Every restart under way.
    fn restart_records(&self) -> Result<Vec<RestartRecord>, StoreError>;
    /// Writes the record whole, as its phase now stands.
    fn record_restart(&self, record: RestartRecord) -> Result<(), StoreError>;
    /// Binds the resumed seat in one write: the flow's route becomes the
    /// birth's, the birth is kept, and the restart record is removed. False,
    /// writing nothing, unless the record is Spawning in the birth's pane.
    fn rebind_restarted(&self, birth: SeatBirth) -> Result<bool, StoreError>;
    /// The seat a restart bound for `flow_id`, if one did.
    fn seat_birth(&self, flow_id: &str) -> Result<Option<SeatBirth>, StoreError>;
    /// Adds the removal of `flow_id`'s restart record, when it has one, to
    /// `commit`: a retired flow holds its Address no longer.
    fn restart_retracted(
        &self,
        flow_id: &str,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError>;
}

impl RecordsRestarts for FlowStore {
    fn restart_record(&self, flow_id: &str) -> Result<Option<RestartRecord>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(self.restarts, RecordKey::new(flow_id)))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [stored] => Ok(Some(stored.restart_record.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn restart_at(&self, address: &Address) -> Result<Option<RestartRecord>, StoreError> {
        Ok(self
            .restart_records()?
            .into_iter()
            .find(|record| &record.address == address))
    }

    fn restart_records(&self) -> Result<Vec<RestartRecord>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.restarts))?
            .records()
            .iter()
            .map(|stored| stored.restart_record.clone())
            .collect())
    }

    fn record_restart(&self, record: RestartRecord) -> Result<(), StoreError> {
        let exists = self.restart_record(&record.flow_id.0)?.is_some();
        let stored = StoredRestart {
            restart_record: record,
        };
        let commit = self.engine.begin_atomic_commit();
        let commit = if exists {
            commit.mutate(self.restarts, stored)
        } else {
            commit.assert(self.restarts, stored)
        };
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(())
    }

    fn rebind_restarted(&self, birth: SeatBirth) -> Result<bool, StoreError> {
        let Some(record) = self.restart_record(&birth.flow_id.0)? else {
            return Ok(false);
        };
        let RestartPhase::Spawning(pane) = &record.restart_phase else {
            return Ok(false);
        };
        if pane.herdr_session_name != birth.herdr_route.herdr_session_name
            || pane.herdr_pane_id != birth.herdr_route.herdr_pane_id
            || pane.herdr_terminal_id != birth.herdr_route.herdr_terminal_id
        {
            return Ok(false);
        }
        let route = FlowHerdrRouteRecord {
            flow_id: birth.flow_id.clone().0,
            route: birth.herdr_route.clone(),
        };
        let commit = self
            .engine
            .begin_atomic_commit()
            .retract(self.restarts, RecordKey::new(birth.flow_id.clone().0));
        let commit = match self.herdr_route(&birth.flow_id.0)? {
            Some(_) => commit.mutate(self.herdr_routes, route),
            None => commit.assert(self.herdr_routes, route),
        };
        let commit = match self.seat_birth(&birth.flow_id.0)? {
            Some(_) => commit.mutate(self.seat_births, birth),
            None => commit.assert(self.seat_births, birth),
        };
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(true)
    }

    fn seat_birth(&self, flow_id: &str) -> Result<Option<SeatBirth>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(self.seat_births, RecordKey::new(flow_id)))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [birth] => Ok(Some(birth.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn restart_retracted(
        &self,
        flow_id: &str,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError> {
        Ok(match self.restart_record(flow_id)? {
            Some(_) => commit.retract(self.restarts, RecordKey::new(flow_id)),
            None => commit,
        })
    }
}
