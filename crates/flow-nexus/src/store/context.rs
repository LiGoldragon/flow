//! How big each flow's context was when its harness last stopped: the
//! tokens its native session carried into its last turn, read from the
//! session's transcript when the flow's hook reports `Stopped`. One row per
//! flow, keyed by its FlowId, rewritten at each Stop. A wake reads it to
//! choose between resuming the session and launching fresh.
//!
//! The row lives in its own table, so a store written before context sizes
//! were kept opens with the table added and its flows simply have none.

use super::metaflow::SleepsMetaflows;
use super::{FlowStore, ManagesMetaflowState, ReadsLaunchAttempt, StoreError};
use meta_signal_flow::{CompactionRefusal, ContextBelowThreshold};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, EngineRecord, FamilyName, KeyedMutation, QueryPlan, RecordKey, SchemaHash,
    TableDescriptor, TableName, TableReference,
};

pub(super) const FLOW_CONTEXT_SIZE_TABLE_NAME: TableName =
    TableName::new("flow_nexus_flow_context_sizes");
pub(super) const FLOW_CONTEXT_SIZE_FAMILY: &str = "flow-nexus-flow-context-size";
pub(super) const FLOW_CONTEXT_SIZE_SCHEMA: &str = "flow-nexus-flow-context-size-v1";

/// One flow's context size at its last Stop, in tokens.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct FlowContextSize {
    pub flow_id: String,
    pub tokens: i64,
}

impl EngineRecord for FlowContextSize {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

pub(super) trait DescribesContextSizeTable {
    fn descriptor() -> TableDescriptor<FlowContextSize>;
}

impl DescribesContextSizeTable for FlowContextSize {
    fn descriptor() -> TableDescriptor<FlowContextSize> {
        TableDescriptor::new(
            FLOW_CONTEXT_SIZE_TABLE_NAME,
            FamilyName::new(FLOW_CONTEXT_SIZE_FAMILY),
            SchemaHash::for_label(FLOW_CONTEXT_SIZE_SCHEMA),
        )
    }
}

pub trait RecordsContextSize {
    /// Writes the flow's context size at its Stop, replacing the last one.
    fn record_context_size(&self, flow_id: &str, tokens: i64) -> Result<(), StoreError>;
    /// The flow's context size at its last Stop; None when none was read.
    fn context_size(&self, flow_id: &str) -> Result<Option<i64>, StoreError>;
}

trait ReadsContextSizeRow {
    fn context_size_row(&self, flow_id: &str) -> Result<Option<FlowContextSize>, StoreError>;
    fn context_sizes(&self) -> TableReference<FlowContextSize>;
}

impl ReadsContextSizeRow for FlowStore {
    fn context_sizes(&self) -> TableReference<FlowContextSize> {
        self.context_sizes
    }

    fn context_size_row(&self, flow_id: &str) -> Result<Option<FlowContextSize>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.context_sizes(),
                RecordKey::new(flow_id),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [row] => Ok(Some(row.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }
}

impl RecordsContextSize for FlowStore {
    fn record_context_size(&self, flow_id: &str, tokens: i64) -> Result<(), StoreError> {
        let row = FlowContextSize {
            flow_id: flow_id.into(),
            tokens,
        };
        if self.context_size_row(flow_id)?.is_some() {
            self.engine.mutate_keyed(KeyedMutation::new(
                self.context_sizes(),
                RecordKey::new(flow_id),
                row,
            ))?;
        } else {
            self.engine
                .assert(Assertion::new(self.context_sizes(), row))?;
        }
        Ok(())
    }

    fn context_size(&self, flow_id: &str) -> Result<Option<i64>, StoreError> {
        Ok(self.context_size_row(flow_id)?.map(|row| row.tokens))
    }
}

/// Compaction is an emergency measure, refused while the context is below
/// a threshold: a flow is compacted only when its context at its last Stop
/// was at or above the Refresh threshold configured for its layer, the
/// point past which the flow should already have been refreshed. Its layer
/// is the one of the metaflow Awake as it, else the one its launch named.
pub trait JudgesCompaction {
    /// Why a Compact of this flow is refused; None when it may compact.
    fn compaction_refusal(&self, flow_id: &str) -> Result<Option<CompactionRefusal>, StoreError>;
}

impl JudgesCompaction for FlowStore {
    fn compaction_refusal(&self, flow_id: &str) -> Result<Option<CompactionRefusal>, StoreError> {
        let layer = match self.metaflow_awake_as(flow_id)? {
            Some(metaflow) => Some(metaflow.address.layer),
            None => match self.launch_requests_bound_to(flow_id)?.pop() {
                Some(launch_request_id) => self
                    .launch_attempt(&launch_request_id)?
                    .map(|attempt| attempt.launch_profile.address.layer),
                None => None,
            },
        };
        let threshold = match layer {
            Some(layer) => self.configured_threshold(&layer)?,
            None => None,
        };
        let Some(threshold) = threshold else {
            return Ok(Some(CompactionRefusal::NoThreshold));
        };
        let Some(tokens) = self.context_size(flow_id)? else {
            return Ok(Some(CompactionRefusal::Unmeasured));
        };
        Ok(
            (tokens < threshold.refresh).then_some(CompactionRefusal::BelowThreshold(
                ContextBelowThreshold {
                    context_tokens: tokens,
                    refresh_tokens: threshold.refresh,
                },
            )),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::RecordsContextSize;
    use crate::store::{FlowStore, OpensFlowStore};

    #[test]
    fn a_flow_context_size_is_rewritten_at_each_stop_and_kept_across_a_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("flow.sema");
        {
            let store = FlowStore::open(&path).unwrap();
            assert_eq!(store.context_size("5a4d0b").unwrap(), None);
            store.record_context_size("5a4d0b", 120_000).unwrap();
            store.record_context_size("5a4d0b", 151_000).unwrap();
        }
        let store = FlowStore::open(&path).unwrap();
        assert_eq!(store.context_size("5a4d0b").unwrap(), Some(151_000));
        assert_eq!(store.context_size("0a0a0a").unwrap(), None);
    }
}
