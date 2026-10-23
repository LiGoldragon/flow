//! The successors Flow owes. A metaflow whose declaration is Maintained
//! always has a flow: when its flow is refreshed, or ends without Flow or
//! an owner ending it (its pane gone), Flow records that the metaflow is
//! owed a successor, in the write that ends the flow or before a refresh
//! starts the successor, and starts it with no queued request needed. The
//! intent goes in the write in which a flow takes the metaflow's Address,
//! so a Nexus that dies between the two finds the intent at its next start
//! and starts the successor then.
//!
//! The rows live in their own table, so a store written before successors
//! were owed opens with the table added and holding none.

use super::{FlowStore, ReadsFlowMemory, StoreError};
use meta_signal_flow::Continuity;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    AtomicCommit, EngineRecord, FamilyName, QueryPlan, RecordKey, SchemaHash, TableDescriptor,
    TableName,
};
use signal_flow::Address;

pub(super) const SUCCESSOR_INTENT_TABLE_NAME: TableName =
    TableName::new("flow_nexus_successor_intents");
pub(super) const SUCCESSOR_INTENT_FAMILY: &str = "flow-nexus-successor-intent";
pub(super) const SUCCESSOR_INTENT_SCHEMA: &str = "flow-nexus-successor-intent-v1";

/// Why a Maintained metaflow is owed its successor.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum SuccessionCause {
    /// Its flow passed its model's refresh threshold and rested.
    Refresh,
    /// Its flow ended without Flow or an owner ending it: its pane gone.
    Exit,
}

/// A successor owed to the metaflow at `address`, after `predecessor`. A
/// refresh's successor replaces its predecessor under `launch_request_id`,
/// so a Nexus that takes the intent up again resumes that replacement
/// rather than starting a second; an exit's successor is a wake, which
/// names none.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct SuccessorIntent {
    pub address: Address,
    pub predecessor: String,
    pub cause: SuccessionCause,
    pub launch_request_id: String,
}

impl EngineRecord for SuccessorIntent {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.address))
    }
}

pub(super) trait DescribesSuccessorIntentTable {
    fn descriptor() -> TableDescriptor<SuccessorIntent>;
}

impl DescribesSuccessorIntentTable for SuccessorIntent {
    fn descriptor() -> TableDescriptor<SuccessorIntent> {
        TableDescriptor::new(
            SUCCESSOR_INTENT_TABLE_NAME,
            FamilyName::new(SUCCESSOR_INTENT_FAMILY),
            SchemaHash::for_label(SUCCESSOR_INTENT_SCHEMA),
        )
    }
}

/// How a metaflow continues, as its declaration names it; a metaflow no
/// declaration names is OnDemand.
pub(crate) trait ReadsContinuity {
    fn continuity(&self, address: &Address) -> Result<Continuity, StoreError>;
}

impl ReadsContinuity for FlowStore {
    fn continuity(&self, address: &Address) -> Result<Continuity, StoreError> {
        use super::metaflow_declaration::ReadsMetaflowDeclarations;
        Ok(self
            .metaflow_declaration(address)?
            .map(|declared| declared.continuity)
            .unwrap_or(Continuity::OnDemand))
    }
}

pub(crate) trait OwesSuccessors {
    /// The successor owed to the metaflow at `address`, if any.
    fn successor_intent(&self, address: &Address) -> Result<Option<SuccessorIntent>, StoreError>;
    /// Every successor owed.
    fn successor_intents(&self) -> Result<Vec<SuccessorIntent>, StoreError>;
    /// Records the successor owed, in its own write; an intent already
    /// recorded for the Address is replaced.
    fn owe_successor(&self, intent: SuccessorIntent) -> Result<(), StoreError>;
    /// Adds the intent to a commit that ends its predecessor.
    fn owing_successor(
        &self,
        intent: SuccessorIntent,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError>;
    /// Adds the retraction of the intent at `address`, if one is recorded,
    /// to a commit in which a flow takes the Address.
    fn successor_given(
        &self,
        address: &Address,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError>;
    /// Retracts the intent at `address`, in its own write.
    fn forget_successor(&self, address: &Address) -> Result<(), StoreError>;
}

impl OwesSuccessors for FlowStore {
    fn successor_intent(&self, address: &Address) -> Result<Option<SuccessorIntent>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.successor_intents,
                RecordKey::new(FlowStore::address_record_key(address)),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [intent] => Ok(Some(intent.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn successor_intents(&self) -> Result<Vec<SuccessorIntent>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.successor_intents))?
            .records()
            .to_vec())
    }

    fn owe_successor(&self, intent: SuccessorIntent) -> Result<(), StoreError> {
        let commit = self.owing_successor(intent, self.engine.begin_atomic_commit())?;
        self.engine.commit_atomic(commit)?;
        Ok(())
    }

    fn owing_successor(
        &self,
        intent: SuccessorIntent,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError> {
        Ok(match self.successor_intent(&intent.address)? {
            Some(_) => commit.mutate(self.successor_intents, intent),
            None => commit.assert(self.successor_intents, intent),
        })
    }

    fn successor_given(
        &self,
        address: &Address,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError> {
        Ok(match self.successor_intent(address)? {
            Some(_) => commit.retract(
                self.successor_intents,
                RecordKey::new(FlowStore::address_record_key(address)),
            ),
            None => commit,
        })
    }

    fn forget_successor(&self, address: &Address) -> Result<(), StoreError> {
        if self.successor_intent(address)?.is_none() {
            return Ok(());
        }
        let commit = self.successor_given(address, self.engine.begin_atomic_commit())?;
        self.engine.commit_atomic(commit)?;
        Ok(())
    }
}
