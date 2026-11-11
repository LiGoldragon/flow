//! The last turn end of each harness: a Claude flow's Stop in its own
//! session, or a Codex flow's thread reporting a turn completed. One record
//! per harness, replaced at each turn end, so `Observe.TurnEnds` opens with
//! the state it names even after a restart; and one per Address, the last
//! turn end of its flows, which `Hot` reads.

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

/// The key of an Address's last turn end, apart from the harnesses' keys.
pub(crate) trait KeysAddressTurnEnd {
    fn turn_end_key(&self) -> String;
}

impl KeysAddressTurnEnd for signal_flow::Address {
    fn turn_end_key(&self) -> String {
        use super::ReadsFlowMemory;
        format!("address/{}", FlowStore::address_record_key(self))
    }
}

pub(crate) trait RecordsTurnEnds {
    /// Makes `turn_end` the last of its harness and of its Address.
    fn record_turn_end(&self, turn_end: TurnEnd) -> Result<(), StoreError> {
        let address_key = turn_end.address.turn_end_key();
        self.record_turn_end_under(turn_end.harness_kind.key(), turn_end.clone())?;
        self.record_turn_end_under(&address_key, turn_end)
    }
    /// Makes `turn_end` the one held under `key`.
    fn record_turn_end_under(&self, key: &str, turn_end: TurnEnd) -> Result<(), StoreError>;
    /// The last turn end of each harness that has one, Claude before Codex.
    fn last_turn_ends(&self) -> Result<Vec<TurnEnd>, StoreError>;
    /// The last turn end of the flows of `address`, if one is held.
    fn last_turn_end_of(
        &self,
        address: &signal_flow::Address,
    ) -> Result<Option<TurnEnd>, StoreError>;
}

impl RecordsTurnEnds for FlowStore {
    fn record_turn_end_under(&self, key: &str, turn_end: TurnEnd) -> Result<(), StoreError> {
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
        records.retain(|record| {
            [HarnessKind::Claude.key(), HarnessKind::Codex.key()].contains(&record.key.as_str())
        });
        records.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(records.into_iter().map(|record| record.turn_end).collect())
    }

    fn last_turn_end_of(
        &self,
        address: &signal_flow::Address,
    ) -> Result<Option<TurnEnd>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(
                self.turn_ends,
                RecordKey::new(address.turn_end_key()),
            ))?
            .records()
            .first()
            .map(|record| record.turn_end.clone()))
    }
}
