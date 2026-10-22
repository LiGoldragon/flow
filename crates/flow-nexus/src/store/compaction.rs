//! The compactions a flow's harness made on its own while the flow kept
//! working, each recorded once: the flow, the harness's own id for the
//! compaction, the metaflow it ran as, and what became of the
//! investigation Flow raised for it. A compaction already recorded raises
//! nothing again, so a transcript read twice, or read again after a Nexus
//! restart, warns once.
//!
//! The rows live in their own table, so a store written before compactions
//! were kept opens with the table added and holding none.

use super::{FlowStore, StoreError};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use memory_engine::{
    Assertion, EngineRecord, FamilyName, KeyedMutation, QueryPlan, RecordKey, SchemaHash,
    TableDescriptor, TableName,
};
use signal_flow::Address;

pub(super) const COMPACTION_TABLE_NAME: TableName = TableName::new("flow_nexus_compactions");
pub(super) const COMPACTION_FAMILY: &str = "flow-nexus-compaction";
pub(super) const COMPACTION_SCHEMA: &str = "flow-nexus-compaction-v1";

/// What Flow did with the warning a compaction raised.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum Investigation {
    /// The Investigation seat was started fresh with it as its brief.
    Started(String),
    /// The Investigation seat was Awake; the investigation was queued for it.
    Queued,
    /// No Investigation seat is configured: the warning was logged only.
    NotConfigured,
    /// The investigation could not be started or queued; why.
    Refused(String),
}

/// One compaction a flow's harness made while the flow kept working.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct Compaction {
    pub flow_id: String,
    /// The harness's own id for the compaction: a Claude `compact_boundary`
    /// row's uuid, a Codex `contextCompaction` item's id.
    pub boundary: String,
    pub address: Option<Address>,
    pub investigation: Investigation,
}

impl EngineRecord for Compaction {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(Compaction::key(&self.flow_id, &self.boundary))
    }
}

pub trait KeysCompaction {
    fn key(flow_id: &str, boundary: &str) -> String;
}

impl KeysCompaction for Compaction {
    fn key(flow_id: &str, boundary: &str) -> String {
        format!("{flow_id}:{boundary}")
    }
}

pub(super) trait DescribesCompactionTable {
    fn descriptor() -> TableDescriptor<Compaction>;
}

impl DescribesCompactionTable for Compaction {
    fn descriptor() -> TableDescriptor<Compaction> {
        TableDescriptor::new(
            COMPACTION_TABLE_NAME,
            FamilyName::new(COMPACTION_FAMILY),
            SchemaHash::for_label(COMPACTION_SCHEMA),
        )
    }
}

pub trait RecordsCompactions {
    /// Whether this compaction of this flow is already recorded.
    fn compaction_recorded(&self, flow_id: &str, boundary: &str) -> Result<bool, StoreError>;
    /// Records a compaction once; false when it was already recorded.
    fn record_compaction(&self, compaction: Compaction) -> Result<bool, StoreError>;
    /// Rewrites a recorded compaction with what became of its
    /// investigation.
    fn settle_compaction(&self, compaction: Compaction) -> Result<(), StoreError>;
    /// Every compaction recorded for a flow.
    fn compactions_of(&self, flow_id: &str) -> Result<Vec<Compaction>, StoreError>;
}

impl RecordsCompactions for FlowStore {
    fn compaction_recorded(&self, flow_id: &str, boundary: &str) -> Result<bool, StoreError> {
        Ok(!self
            .engine
            .match_records(QueryPlan::key(
                self.compactions,
                RecordKey::new(Compaction::key(flow_id, boundary)),
            ))?
            .records()
            .is_empty())
    }

    fn record_compaction(&self, compaction: Compaction) -> Result<bool, StoreError> {
        if self.compaction_recorded(&compaction.flow_id, &compaction.boundary)? {
            return Ok(false);
        }
        self.engine
            .assert(Assertion::new(self.compactions, compaction))?;
        Ok(true)
    }

    fn settle_compaction(&self, compaction: Compaction) -> Result<(), StoreError> {
        let key = RecordKey::new(Compaction::key(&compaction.flow_id, &compaction.boundary));
        self.engine
            .mutate_keyed(KeyedMutation::new(self.compactions, key, compaction))?;
        Ok(())
    }

    fn compactions_of(&self, flow_id: &str) -> Result<Vec<Compaction>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.compactions))?
            .records()
            .iter()
            .filter(|compaction| compaction.flow_id == flow_id)
            .cloned()
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::{Compaction, Investigation, RecordsCompactions};
    use crate::store::{FlowStore, OpensFlowStore};

    #[test]
    fn a_compaction_is_recorded_once_and_kept_across_a_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("flow.memory");
        let compaction = Compaction {
            flow_id: "5a4d0b".into(),
            boundary: "b1".into(),
            address: None,
            investigation: Investigation::NotConfigured,
        };
        {
            let store = FlowStore::open(&path).unwrap();
            assert!(!store.compaction_recorded("5a4d0b", "b1").unwrap());
            assert!(store.record_compaction(compaction.clone()).unwrap());
            assert!(!store.record_compaction(compaction.clone()).unwrap());
            store
                .settle_compaction(Compaction {
                    investigation: Investigation::Queued,
                    ..compaction.clone()
                })
                .unwrap();
        }
        let store = FlowStore::open(&path).unwrap();
        assert!(store.compaction_recorded("5a4d0b", "b1").unwrap());
        assert_eq!(
            store.compactions_of("5a4d0b").unwrap(),
            vec![Compaction {
                investigation: Investigation::Queued,
                ..compaction
            }]
        );
        assert!(store.compactions_of("0a0a0a").unwrap().is_empty());
    }
}
