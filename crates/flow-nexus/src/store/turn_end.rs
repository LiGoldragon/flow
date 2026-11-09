//! The last turn end of each harness: a Claude flow's Stop in its own
//! session, or a Codex flow's thread reporting a turn completed. One record
//! per harness, replaced at each turn end, so `Observe.TurnEnds` opens with
//! the state it names even after a restart.

use super::{FlowStore, StoreError};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, EngineRecord, FamilyName, KeyedMutation, MemoryDefinition, MemoryVersion, QueryPlan,
    RecordKey, SchemaHash, TableDescriptor, TableName,
};
use signal_flow::{HarnessKind, TurnEnd};

pub(super) const TURN_END_TABLE_NAME: TableName = TableName::new("flow_nexus_turn_ends");
pub(super) const TURN_END_FAMILY: &str = "flow-nexus-turn-end";
pub(super) const TURN_END_SCHEMA: &str = "flow-nexus-turn-end-v1";

/// The last turn end of one harness, keyed by the harness.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct TurnEndRecord {
    pub key: String,
    pub turn_end: TurnEnd,
}

impl EngineRecord for TurnEndRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.key.clone())
    }
}

impl MemoryDefinition for TurnEndRecord {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(TURN_END_SCHEMA)
    }
}

pub(super) trait DescribesTurnEndTable {
    fn descriptor() -> TableDescriptor<TurnEndRecord>;
}

impl DescribesTurnEndTable for TurnEndRecord {
    fn descriptor() -> TableDescriptor<TurnEndRecord> {
        TableDescriptor::new(
            TURN_END_TABLE_NAME,
            FamilyName::new(TURN_END_FAMILY),
            SchemaHash::for_label(TURN_END_SCHEMA),
        )
    }
}

/// A harness's place in the state `Observe.TurnEnds` opens with: Claude,
/// then Codex.
pub(crate) trait RanksHarness {
    fn key(&self) -> &'static str;
}

impl RanksHarness for HarnessKind {
    fn key(&self) -> &'static str {
        match self {
            HarnessKind::Claude => "1-Claude",
            HarnessKind::Codex => "2-Codex",
        }
    }
}

pub(crate) trait RecordsTurnEnds {
    /// Makes `turn_end` the last of its harness.
    fn record_turn_end(&self, turn_end: TurnEnd) -> Result<(), StoreError>;
    /// The last turn end of each harness that has one, Claude before Codex.
    fn last_turn_ends(&self) -> Result<Vec<TurnEnd>, StoreError>;
}

impl RecordsTurnEnds for FlowStore {
    fn record_turn_end(&self, turn_end: TurnEnd) -> Result<(), StoreError> {
        let key = turn_end.harness_kind.key();
        let record = TurnEndRecord {
            key: key.to_owned(),
            turn_end,
        };
        let held = self
            .engine
            .match_records(QueryPlan::key(self.turn_ends, RecordKey::new(key)))?
            .records()
            .len();
        if held > 1 {
            return Err(StoreError::StateInvariant);
        }
        if held == 1 {
            self.engine.mutate_keyed(KeyedMutation::new(
                self.turn_ends,
                RecordKey::new(key),
                record,
            ))?;
        } else {
            self.engine.assert(Assertion::new(self.turn_ends, record))?;
        }
        Ok(())
    }

    fn last_turn_ends(&self) -> Result<Vec<TurnEnd>, StoreError> {
        let mut records = self
            .engine
            .match_records(QueryPlan::all(self.turn_ends))?
            .records()
            .to_vec();
        records.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(records.into_iter().map(|record| record.turn_end).collect())
    }
}
