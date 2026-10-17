//! A metaflow's fresh-wake mark. A privileged Refresh marks one Asleep
//! metaflow; its next wake then composes a new native session from the
//! modules and leave-outs its last launch recorded instead of resuming the
//! last flow's session. The mark is one row per Address, in its own table, so
//! a store written before marks were kept opens with the table added and
//! holds none. `Record.Awoken` retracts it in the write that records the
//! wake, so a mark governs exactly one wake.

use super::{FlowStore, ManagesMetaflowState, MetaflowState, ReadsFlowMemory, StoreError};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, AtomicCommit, EngineRecord, FamilyName, QueryPlan, RecordKey, SchemaHash,
    TableDescriptor, TableName,
};
use signal_flow::Address;

pub(super) const METAFLOW_REFRESH_TABLE_NAME: TableName =
    TableName::new("flow_nexus_metaflow_refreshes");
pub(super) const METAFLOW_REFRESH_FAMILY: &str = "flow-nexus-metaflow-refresh";
pub(super) const METAFLOW_REFRESH_SCHEMA: &str = "flow-nexus-metaflow-refresh-v1";

/// The Address whose next wake is fresh.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct MetaflowRefresh {
    pub address: Address,
}

impl EngineRecord for MetaflowRefresh {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.address))
    }
}

pub(super) trait DescribesRefreshTable {
    fn descriptor() -> TableDescriptor<MetaflowRefresh>;
}

impl DescribesRefreshTable for MetaflowRefresh {
    fn descriptor() -> TableDescriptor<MetaflowRefresh> {
        TableDescriptor::new(
            METAFLOW_REFRESH_TABLE_NAME,
            FamilyName::new(METAFLOW_REFRESH_FAMILY),
            SchemaHash::for_label(METAFLOW_REFRESH_SCHEMA),
        )
    }
}

pub(crate) trait MarksFreshWakes {
    /// Marks the metaflow at `address` for a fresh wake: true once it is
    /// marked (a mark already held is left as it is), false when the
    /// metaflow is absent or not Asleep.
    fn mark_fresh_wake(&self, address: &Address) -> Result<bool, StoreError>;
    /// Whether the metaflow's next wake is fresh.
    fn fresh_wake_marked(&self, address: &Address) -> Result<bool, StoreError>;
    /// Adds the retraction of the mark, when there is one, to a commit.
    fn fresh_wake_cleared(
        &self,
        address: &Address,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError>;
}

impl MarksFreshWakes for FlowStore {
    fn mark_fresh_wake(&self, address: &Address) -> Result<bool, StoreError> {
        let asleep = self
            .stored_metaflow(address)?
            .is_some_and(|stored| stored.metaflow.state == MetaflowState::Asleep);
        if !asleep {
            return Ok(false);
        }
        if !self.fresh_wake_marked(address)? {
            self.engine.assert(Assertion::new(
                self.refreshes,
                MetaflowRefresh {
                    address: address.clone(),
                },
            ))?;
        }
        Ok(true)
    }

    fn fresh_wake_marked(&self, address: &Address) -> Result<bool, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.refreshes,
                RecordKey::new(FlowStore::address_record_key(address)),
            ))?
            .records()
            .len();
        match records {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn fresh_wake_cleared(
        &self,
        address: &Address,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError> {
        Ok(if self.fresh_wake_marked(address)? {
            commit.retract(
                self.refreshes,
                RecordKey::new(FlowStore::address_record_key(address)),
            )
        } else {
            commit
        })
    }
}
